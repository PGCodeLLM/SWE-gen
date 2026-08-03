"""Dependency-light HTTP server for the distributed pipeline dashboard."""

from __future__ import annotations

import hmac
import json
import secrets
import subprocess
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from swegen.dashboard.distributed_status import (
    K3sStatusCollector,
    PipelineStatusCollector,
    RemoteBuildKitFarmCollector,
)

SCALE_MIN = 0
BUILD_SLOT_MIN = 1
GENERATE_MAIN_CAPACITY = 92
PRIMARY_DEPLOYMENTS = {
    "validate": "swegen-validate",
    "repair": "swegen-repair",
    "reward": "swegen-reward",
    "push": "swegen-push",
}


class ScalingBusyError(RuntimeError):
    """Raised when another scaling operation is already in flight."""


class K3sScaler:
    """Strictly allowlisted, argv-only deployment scaling."""

    def __init__(
        self,
        *,
        namespace: str = "swegen-pipeline",
        runner: Any = subprocess.run,
    ) -> None:
        self.namespace = namespace
        self.runner = runner
        self._lock = threading.Lock()

    @staticmethod
    def plan(
        stage: object,
        replicas: object,
        *,
        max_replicas: object,
    ) -> list[tuple[str, int]]:
        if not isinstance(stage, str) or stage not in {
            "generate",
            *PRIMARY_DEPLOYMENTS,
        }:
            raise ValueError("unknown stage")
        if isinstance(replicas, bool) or not isinstance(replicas, int):
            raise ValueError("replicas must be an integer")
        if (
            isinstance(max_replicas, bool)
            or not isinstance(max_replicas, int)
            or max_replicas < SCALE_MIN
        ):
            raise ValueError("cluster scaling capacity is unavailable")
        if not SCALE_MIN <= replicas <= max_replicas:
            raise ValueError(f"replicas must be between {SCALE_MIN} and {max_replicas}")
        if stage == "generate":
            main = min(replicas, GENERATE_MAIN_CAPACITY)
            overflow = max(0, replicas - GENERATE_MAIN_CAPACITY)
            if replicas < GENERATE_MAIN_CAPACITY:
                return [("swegen-generate-overflow", 0), ("swegen-generate", main)]
            return [("swegen-generate", main), ("swegen-generate-overflow", overflow)]
        return [(PRIMARY_DEPLOYMENTS[stage], replicas)]

    def scale(
        self,
        stage: object,
        replicas: object,
        *,
        max_replicas: object,
    ) -> list[dict[str, Any]]:
        plan = self.plan(stage, replicas, max_replicas=max_replicas)
        if not self._lock.acquire(blocking=False):
            raise ScalingBusyError("another scaling request is already running")
        try:
            applied = []
            for deployment, count in plan:
                command = [
                    "kubectl",
                    "--request-timeout=10s",
                    "-n",
                    self.namespace,
                    "scale",
                    f"deployment/{deployment}",
                    f"--replicas={count}",
                ]
                completed = self.runner(
                    command,
                    capture_output=True,
                    text=True,
                    timeout=15,
                    check=False,
                )
                if completed.returncode != 0:
                    message = (completed.stderr or "kubectl scale failed")[:500]
                    raise RuntimeError(message)
                applied.append({"deployment": deployment, "replicas": count})
            return applied
        finally:
            self._lock.release()


class BuildSlotBusyError(RuntimeError):
    """Raised when another node-local BuildKit slot update is in flight."""


class K3sBuildSlotController:
    """Update an allowlisted node's mounted BuildKit slot count atomically."""

    _UPDATE_SCRIPT = """import json,os,pathlib,sys
d=pathlib.Path('/run/swegen-build-slots')
n=int(sys.argv[1])
if n < 1:
 raise ValueError('slot count must be positive')
d.mkdir(parents=True,exist_ok=True)
for i in range(n):
 (d/str(i)).touch(exist_ok=True)
tmp=d/f'.count.{os.getpid()}.tmp'
tmp.write_text(f'{n}\\n',encoding='utf-8')
os.replace(tmp,d/'count')
print(json.dumps({'slots':n}))"""

    def __init__(
        self,
        *,
        namespace: str = "swegen-pipeline",
        runner: Any = subprocess.run,
    ) -> None:
        self.namespace = namespace
        self.runner = runner
        self._lock = threading.Lock()

    @staticmethod
    def plan(
        node: object,
        slots: object,
        *,
        nodes: object,
    ) -> tuple[str, int]:
        if not isinstance(node, str) or not node:
            raise ValueError("node must be a non-empty string")
        if isinstance(slots, bool) or not isinstance(slots, int):
            raise ValueError("slots must be an integer")
        if not isinstance(nodes, list):
            raise ValueError("cluster node status is unavailable")
        node_status = next(
            (entry for entry in nodes if isinstance(entry, dict) and entry.get("name") == node),
            None,
        )
        if node_status is None:
            raise ValueError("unknown node")
        max_slots = node_status.get("build_slot_max")
        if isinstance(max_slots, bool) or not isinstance(max_slots, int) or max_slots < 1:
            raise ValueError("node BuildKit slot capacity is unavailable")
        if not BUILD_SLOT_MIN <= slots <= max_slots:
            raise ValueError(
                f"slots must be between {BUILD_SLOT_MIN} and {max_slots} for this node"
            )
        probe_pod = node_status.get("build_slot_probe_pod")
        if not isinstance(probe_pod, str) or not probe_pod:
            raise ValueError("node BuildKit slot controller is unavailable")
        return probe_pod, max_slots

    def update(
        self,
        node: object,
        slots: object,
        *,
        nodes: object,
    ) -> dict[str, Any]:
        probe_pod, max_slots = self.plan(node, slots, nodes=nodes)
        if not self._lock.acquire(blocking=False):
            raise BuildSlotBusyError("another BuildKit slot update is already running")
        try:
            command = [
                "kubectl",
                "--request-timeout=10s",
                "-n",
                self.namespace,
                "exec",
                probe_pod,
                "--",
                "python",
                "-c",
                self._UPDATE_SCRIPT,
                str(slots),
            ]
            completed = self.runner(
                command,
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            if completed.returncode != 0:
                message = (completed.stderr or "BuildKit slot update failed")[:500]
                raise RuntimeError(message)
            return {
                "node": node,
                "slots": slots,
                "max_slots": max_slots,
                "controller_pod": probe_pod,
            }
        finally:
            self._lock.release()


class SnapshotCache:
    def __init__(self, *, refresh_seconds: float = 5.0) -> None:
        self.refresh_seconds = refresh_seconds
        self._lock = threading.Lock()
        self._stopped = threading.Event()
        self._snapshot: dict[str, Any] = {
            "postgres": {},
            "k3s": {},
            "buildkit_farm": {},
            "sources": {},
        }
        self._collectors = {
            "postgres": PipelineStatusCollector(),
            "k3s": K3sStatusCollector(),
            "buildkit_farm": RemoteBuildKitFarmCollector(),
        }

    def refresh(self) -> None:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            current = dict(self._snapshot)
            sources = dict(current.get("sources", {}))
        for name, collector in self._collectors.items():
            try:
                current[name] = collector.collect()
                sources[name] = {"ok": True, "fetched_at": now, "error": None}
            except Exception as error:
                if name == "k3s" and current.get("k3s"):
                    retained = json.loads(json.dumps(current["k3s"], default=str))
                    scaling = dict(retained.get("scaling", {}))
                    configured_max = max(
                        (
                            int(stage.get("desired") or 0)
                            for stage in retained.get("stages", {}).values()
                        ),
                        default=0,
                    )
                    scaling["max_replicas"] = max(
                        int(scaling.get("max_replicas") or 0),
                        configured_max,
                    )
                    scaling["stale"] = True
                    retained["scaling"] = scaling
                    current["k3s"] = retained
                sources[name] = {
                    "ok": False,
                    "fetched_at": sources.get(name, {}).get("fetched_at"),
                    "error": f"{type(error).__name__}: {str(error)[:300]}",
                }
        current["sources"] = sources
        current["generated_at"] = now
        with self._lock:
            self._snapshot = current

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return json.loads(json.dumps(self._snapshot, default=str))

    def run(self) -> None:
        while not self._stopped.is_set():
            self.refresh()
            self._stopped.wait(self.refresh_seconds)

    def stop(self) -> None:
        self._stopped.set()


HTML = r"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><meta name="csrf-token" content="__CSRF_TOKEN__">
<title>SWE-gen k3s Pipeline</title><style>
:root{color-scheme:dark;--bg:#07111f;--card:#102139;--muted:#91a4bd;--ok:#32d583;--bad:#ff6b6b;--line:#233955}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:#edf4ff;font:14px system-ui,sans-serif}
main{max-width:1500px;margin:auto;padding:12px}h1{font-size:24px;margin:0 0 2px}h2{font-size:18px;margin:14px 0 6px}.muted{color:var(--muted)}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:6px;margin:8px 0}
.card{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:8px}.big{font-size:25px;font-weight:700}
.pipeline-flow{display:grid;grid-template-columns:minmax(235px,1fr) minmax(520px,2fr) minmax(235px,1fr) minmax(235px,1fr);gap:6px;align-items:stretch;margin:8px 0;overflow-x:auto;padding-bottom:4px}
.pipeline-flow>.stage-card{min-width:235px}.stage-card{display:flex;flex-direction:column;padding:11px}.stage-stats{min-width:0}.stage-stats>b{display:block;margin-bottom:3px}.stage-stats>.big{margin-bottom:1px}.stage-stats>.pod-phases{color:var(--muted);font-size:11px;line-height:1.3;margin-bottom:4px;overflow-wrap:anywhere}.stage-stats>div:not(.big):not(.pod-phases):not(.scale-controls){line-height:1.35}.validation-loop{min-width:520px;background:#0c1b2f;border:2px solid #365b82;border-radius:10px;padding:9px;display:grid;grid-template-rows:auto minmax(0,1fr) minmax(0,1fr);gap:8px;align-content:stretch}.validation-loop-title{text-align:center;color:var(--muted);font-size:11px;font-weight:700;letter-spacing:.04em;text-transform:uppercase;margin-bottom:1px}.validation-loop>.stage-card{background:var(--card)}.stage-card-horizontal{display:grid;grid-template-columns:240px minmax(0,1fr);gap:12px;align-items:stretch;padding:10px}.stage-card-horizontal .stage-stats{width:240px;text-align:left;justify-self:start;align-self:start}.stage-card-horizontal .stage-chart-wrap{border-left:1px solid var(--line);padding-left:10px}
.ok{color:var(--ok)}.bad{color:var(--bad)}table{width:100%;border-collapse:collapse;background:var(--card)}
th,td{text-align:left;padding:5px;border-bottom:1px solid var(--line);vertical-align:top}th{color:var(--muted)}
.scroll{overflow:auto;border:1px solid var(--line);border-radius:10px}details summary{cursor:pointer}.banner{padding:10px;border:1px solid var(--bad);border-radius:8px;margin:8px 0}
.success{background:#32d583}.failure{background:#ff6b6b}.stage-chart-wrap{min-width:0;min-height:0;display:flex;flex-direction:column}.stage-chart-title{display:flex;justify-content:space-between;align-items:baseline;gap:6px;color:var(--muted);font-size:10px;line-height:1.2;margin-bottom:4px}.stage-card:not(.stage-card-horizontal) .stage-chart-wrap{flex:1;margin-top:10px;padding-top:8px;border-top:1px solid var(--line)}.chart-frame{min-width:0;min-height:112px;flex:1;display:grid;grid-template-columns:auto minmax(0,1fr);gap:5px}.chart-y-axis{min-width:24px;display:flex;flex-direction:column;justify-content:space-between;padding:5px 0 19px;color:var(--muted);font:9px ui-monospace,SFMono-Regular,Consolas,monospace;text-align:right;font-variant-numeric:tabular-nums}.chart{min-height:112px;min-width:0;display:flex;align-items:stretch;gap:2px;border-bottom:1px solid var(--line);padding:5px 1px 0;overflow-x:auto}.bucket{height:auto;min-width:8px;flex:1;display:grid;grid-template-rows:minmax(0,1fr) 18px}.bar-slot{min-height:0;display:flex;align-items:flex-end}.bar{width:100%;display:flex;flex-direction:column-reverse;justify-content:flex-start;min-height:1px}.segment{width:100%;min-height:0}.x-tick{height:18px;position:relative;color:var(--muted);font:9px ui-monospace,SFMono-Regular,Consolas,monospace}.x-tick::before{content:"";position:absolute;top:0;left:50%;height:4px;border-left:1px solid var(--muted);opacity:.75}.x-tick-label{position:absolute;top:6px;left:50%;transform:translateX(-50%);white-space:nowrap;line-height:1}.bucket:first-child .x-tick-label{left:0;transform:none}.bucket:last-child .x-tick-label{left:auto;right:0;transform:none}.chart-empty{min-height:96px;flex:1;display:grid;place-items:center;text-align:center;color:var(--muted);border-bottom:1px solid var(--line);font-size:11px}
.chart-tooltip{position:fixed;z-index:1000;max-width:min(360px,calc(100vw - 16px));padding:5px 7px;border:1px solid #45658a;border-radius:6px;background:#06101d;color:#edf4ff;box-shadow:0 4px 16px #0009;font-size:11px;line-height:1.3;pointer-events:none;white-space:nowrap}.chart-tooltip[hidden]{display:none}
.yield-list{margin-top:5px}.yield-row{display:grid;grid-template-columns:1fr auto auto;gap:5px;padding:4px 0;border-bottom:1px solid var(--line)}.yield-row:last-child{border-bottom:0}.yield-value{font-variant-numeric:tabular-nums}.yield-percent{min-width:52px;text-align:right;font-weight:700}
.scale-controls{width:100%;max-width:174px;display:grid;grid-template-columns:minmax(76px,1fr) 58px;gap:4px;margin:8px auto 0;align-items:center}.scale-controls input,.scale-controls button{min-width:0;border:1px solid var(--line);border-radius:6px;padding:5px 6px;background:#09182b;color:#edf4ff}.scale-controls button{cursor:pointer;background:#174b78}.scale-controls button:disabled{cursor:wait;opacity:.55}.scale-limit{grid-column:1/-1;color:var(--muted);font-size:10px;line-height:1.2;margin-top:-1px;text-align:center}#scale-feedback{min-height:18px;margin-top:4px}
.slot-cell{display:grid;gap:3px;min-width:190px}.slot-status{font-size:12px;line-height:1.25}.slot-controls{display:grid;grid-template-columns:72px 52px;gap:4px;width:128px;align-items:center}.slot-controls input,.slot-controls button{min-width:0;border:1px solid var(--line);border-radius:6px;padding:4px 5px;background:#09182b;color:#edf4ff}.slot-controls button{cursor:pointer;background:#174b78}.slot-controls button:disabled{cursor:wait;opacity:.55}.slot-limit{grid-column:1/-1;color:var(--muted);font-size:9px;line-height:1.15}#build-slot-feedback{min-height:18px;margin:2px 0 4px}
.warning{padding:8px;border:2px solid #f79009;background:#3b2605;color:#ffd79a;border-radius:8px;margin:6px 0}.path{display:block;max-width:520px;overflow-wrap:anywhere;font:12px ui-monospace,SFMono-Regular,Consolas,monospace;color:#b8d8ff}.storage-note{margin-top:3px;color:var(--muted);font-size:12px}
.farm-status{margin:3px 0 6px}.farm-detail{color:var(--muted);font-size:12px;margin-top:3px;overflow-wrap:anywhere}
.node-details>summary{list-style-position:inside;cursor:pointer}.node-summary{display:grid;grid-template-columns:minmax(250px,1fr) minmax(190px,.75fr) minmax(190px,.75fr) minmax(280px,1.05fr) minmax(190px,.75fr);gap:12px;align-items:center}.pod-list{margin:12px 0 2px 22px;display:grid;gap:5px}.pod-row{display:grid;grid-template-columns:minmax(300px,2fr) 110px 100px 80px;gap:10px;padding:5px 8px;border-left:2px solid var(--line);font:12px ui-monospace,SFMono-Regular,Consolas,monospace}.stage-counts{color:var(--muted);font-size:11px;margin-left:22px}@media(max-width:800px){.node-summary{grid-template-columns:1fr}.pod-row{grid-template-columns:1fr 1fr}.node-table-head{display:none}}
</style></head><body><div id="chart-tooltip" class="chart-tooltip" role="tooltip" hidden></div><main><h1>SWE-gen k3s + PGMQ</h1><div id="stamp" class="muted"></div><div id="errors"></div>
<h2>Stages</h2><div id="scale-feedback" class="muted"></div><div id="stages" class="pipeline-flow"></div>
<h2>Hourly yield</h2><div class="muted">Success / all terminal outcomes (success + failed/rejected)</div><div id="yield" class="grid"></div>
<h2>Cluster resources</h2><div id="resource-status" class="muted"></div><div id="build-slot-feedback" class="muted"></div><div id="resource-summary" class="grid"></div><div id="resource-scroll" class="scroll"><table><thead class="node-table-head"><tr><th>Node / IP and scheduled pods</th><th title="Actual metrics usage / sum of Kubernetes CPU requests / node allocatable CPU. Both percentages use allocatable CPU as denominator.">CPU used / allocated / allocatable</th><th>Memory used / allocatable</th><th>Disk I/O read / write · IOPS · busy</th><th>Local BuildKit slots / waiters</th></tr></thead><tbody id="resource-nodes"></tbody></table></div>
<h2>Harbor task storage</h2><div id="storage-warning"></div><div id="storage-summary" class="grid"></div><div class="scroll"><table><thead><tr><th>Deployment / stage</th><th>Node</th><th>Container path</th><th>Backing storage</th><th>Durability</th></tr></thead><tbody id="storage-mounts"></tbody></table></div>
<h2>Remote BuildKit farm</h2><div id="buildkit-farm-status" class="farm-status muted"></div><div id="buildkit-farm-warning"></div><div id="buildkit-farm-summary" class="grid"></div><div class="scroll"><table><thead><tr><th>Sampled farm node / backend</th><th>Read / write throughput</th><th>Read / write IOPS</th><th>Busy / inflight pressure</th></tr></thead><tbody id="buildkit-farm-disk-io"></tbody></table></div>
<h2>Recent tasks</h2><div class="scroll"><table><thead><tr><th>Task</th><th>State</th><th>Stage</th><th>Elapsed</th><th>Task directory / durable source</th><th>Timeline</th></tr></thead><tbody id="tasks"></tbody></table></div>
<script>
const stages=['generate','validate','repair','reward','push']; const el=id=>document.getElementById(id);
const stageNames={generate:'SWEgen',validate:'NOP / Oracle',repair:'Repair',reward:'Reward hack',push:'SWR push'};
const csrfToken=document.querySelector('meta[name="csrf-token"]').content;
const secs=n=>n==null?'—':n<60?`${Math.round(n)}s`:n<3600?`${(n/60).toFixed(1)}m`:`${(n/3600).toFixed(1)}h`;
const cpu=m=>m==null?'—':`${(m/1000).toFixed(2)} cores`;
const compactCores=m=>Number((m/1000).toFixed(m<1000?3:m<10000?2:1)).toString();
const cpuPart=(value,allocatable)=>Number.isFinite(value)&&Number.isFinite(allocatable)&&allocatable>0?`${compactCores(value)} (${Number((value*100/allocatable).toFixed(1))})%`:'—';
const formatCpuTriple=(used,allocated,allocatable)=>`${cpuPart(used,allocatable)} / ${cpuPart(allocated,allocatable)} / ${Number.isFinite(allocatable)&&allocatable>0?compactCores(allocatable):'—'} cores`;
const memory=b=>b==null?'—':`${(b/1024/1024/1024).toFixed(1)} GiB`;
const bytes=b=>b==null?'—':b<1024*1024?`${(b/1024).toFixed(1)} KiB`:`${(b/1024/1024).toFixed(1)} MiB`;
const rateBytes=b=>b==null?'—':b<1024*1024?`${(b/1024).toFixed(1)} KiB/s`:b<1024*1024*1024?`${(b/1024/1024).toFixed(1)} MiB/s`:`${(b/1024/1024/1024).toFixed(2)} GiB/s`;
const rateOps=n=>n==null?'—':`${Number(n).toFixed(n<10?1:0)}`;
const formatDiskIo=io=>!io?`R — · W — · IOPS —/— · busy —`:`R ${rateBytes(io.read_bytes_per_second)} · W ${rateBytes(io.write_bytes_per_second)} · IOPS ${rateOps(io.read_iops)}/${rateOps(io.write_iops)} · busy ${io.busy_percent==null?'—':io.busy_percent.toFixed(1)+'%'}`;
const formatBuildSlots=slots=>!slots?.available?'slots unavailable':`${slots.used}/${slots.total} used (${slots.utilization_percent?.toFixed(1)??'—'}%) · waiters ${slots.waiters??'unknown'}`;
const formatPodPhases=(phases,evicted)=>{const parts=Object.entries(phases||{}).filter(([phase,count])=>phase!=='Running'&&count>0).map(([phase,count])=>`${phase} ${count}`);if(evicted>0)parts.push(`Evicted ${evicted}`);return parts.length?parts.join(' · '):'no other pod states'};
const compactChartCount=value=>value>=1000000?`${Number((value/1000000).toFixed(1))}m`:value>=1000?`${Number((value/1000).toFixed(1))}k`:String(value);
const compactChartTimestamp=value=>new Date(value).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',hour12:false});
const chartTickEvery=(pointCount,chartWidth)=>Math.max(1,Math.ceil(52/Math.max(8,chartWidth/Math.max(1,pointCount))));
const chartScrollSnapshot=chart=>{const maxScroll=Math.max(0,chart.scrollWidth-chart.clientWidth),left=Math.min(Math.max(chart.scrollLeft,0),maxScroll);return {left,followLatest:maxScroll-left<=4}};
function setText(node,value){node.textContent=value==null?'—':String(value)}
const uiState={chartScroll:{},expandedTasks:new Set(),expandedNodes:new Set(),resourceScroll:{left:0,top:0},scaleDrafts:{},scaling:false,buildSlotDrafts:{},buildSlotUpdating:false};
function captureUiState(){document.querySelectorAll('.chart[data-stage]').forEach(chart=>{if(chart.dataset.restoringScroll!=='true')uiState.chartScroll[chart.dataset.stage]=chartScrollSnapshot(chart)});document.querySelectorAll('#tasks details[data-task-key]').forEach(details=>{if(details.open)uiState.expandedTasks.add(details.dataset.taskKey);else uiState.expandedTasks.delete(details.dataset.taskKey)});document.querySelectorAll('#resource-nodes details[data-node-key]').forEach(details=>{if(details.open)uiState.expandedNodes.add(details.dataset.nodeKey);else uiState.expandedNodes.delete(details.dataset.nodeKey)});const resourceScroll=el('resource-scroll');if(resourceScroll){uiState.resourceScroll={left:resourceScroll.scrollLeft,top:resourceScroll.scrollTop}}}
function chartScrollTarget(saved,maxScroll){return saved===undefined||saved.followLatest?maxScroll:Math.min(Math.max(saved.left,0),maxScroll)}
function restoreChartScroll(chart,stage){const saved=Object.prototype.hasOwnProperty.call(uiState.chartScroll,stage)?uiState.chartScroll[stage]:undefined,followLatest=saved===undefined||saved.followLatest;chart.dataset.restoringScroll='true';const apply=()=>{const maxScroll=Math.max(0,chart.scrollWidth-chart.clientWidth);chart.scrollLeft=chartScrollTarget(saved,maxScroll);uiState.chartScroll[stage]={left:chart.scrollLeft,followLatest}};requestAnimationFrame(()=>requestAnimationFrame(()=>{apply();requestAnimationFrame(()=>{apply();delete chart.dataset.restoringScroll})}))}
function positionChartTooltip(event){const tooltip=el('chart-tooltip'),target=event.currentTarget;let x=event.clientX,y=event.clientY;if(!Number.isFinite(x)||!Number.isFinite(y)||event.type==='focus'){const rect=target.getBoundingClientRect();x=rect.left+rect.width/2;y=rect.top}const gap=12,maxLeft=Math.max(8,window.innerWidth-tooltip.offsetWidth-8),maxTop=Math.max(8,window.innerHeight-tooltip.offsetHeight-8);tooltip.style.left=`${Math.min(Math.max(8,x+gap),maxLeft)}px`;tooltip.style.top=`${Math.min(Math.max(8,y+gap),maxTop)}px`}
function showChartTooltip(event,value){const tooltip=el('chart-tooltip');setText(tooltip,value);tooltip.hidden=false;positionChartTooltip(event)}
function hideChartTooltip(){el('chart-tooltip').hidden=true}
function updateChartTicks(chart){const ticks=[...chart.querySelectorAll('.x-tick')],every=chartTickEvery(ticks.length,chart.clientWidth);ticks.forEach((tick,index)=>{tick.hidden=index!==0&&index!==ticks.length-1&&index%every!==0})}
function scaleControls(stage,desired,maxReplicas){const capacityAvailable=Number.isSafeInteger(maxReplicas)&&maxReplicas>=0;const controls=document.createElement('div');controls.className='scale-controls';const input=document.createElement('input');input.type='number';input.min='0';input.max=capacityAvailable?String(maxReplicas):String(desired||0);input.step='1';input.value=uiState.scaleDrafts[stage]??desired??0;input.setAttribute('aria-label',`${stageNames[stage]} total workers`);input.addEventListener('input',()=>{uiState.scaleDrafts[stage]=input.value});const button=document.createElement('button');button.type='button';button.dataset.capacityAvailable=String(capacityAvailable);button.disabled=uiState.scaling||!capacityAvailable;setText(button,'Apply');button.addEventListener('click',()=>submitScale(stage,input.value,maxReplicas));const limit=document.createElement('span');limit.className='scale-limit';setText(limit,capacityAvailable?`cluster CPU ceiling: ${maxReplicas}`:'cluster CPU ceiling unavailable');controls.append(input,button,limit);return controls}
function buildSlotControls(node,slots){const maxSlots=node.build_slot_max,controllerAvailable=Boolean(node.build_slot_probe_pod)&&slots?.available&&Number.isSafeInteger(maxSlots)&&maxSlots>=1;const cell=document.createElement('div');cell.className='slot-cell';cell.title=slots?.waiters_source?`waiters from ${slots.waiters_source}`:(slots?.error||'Wrapper does not persist waiter depth; unknown is explicit.');const status=document.createElement('span');status.className=`slot-status ${(slots?.utilization_percent??0)>=90?'bad':''}`;setText(status,formatBuildSlots(slots));const controls=document.createElement('div');controls.className='slot-controls';controls.addEventListener('click',event=>event.stopPropagation());const input=document.createElement('input');input.type='number';input.min='1';input.max=controllerAvailable?String(maxSlots):String(slots?.total||1);input.step='1';input.value=uiState.buildSlotDrafts[node.name]??slots?.total??1;input.disabled=!controllerAvailable;input.setAttribute('aria-label',`${node.name} local BuildKit slots`);input.addEventListener('input',()=>{uiState.buildSlotDrafts[node.name]=input.value});const button=document.createElement('button');button.type='button';button.dataset.controllerAvailable=String(controllerAvailable);button.disabled=uiState.buildSlotUpdating||!controllerAvailable;setText(button,'Apply');button.addEventListener('click',()=>submitBuildSlots(node.name,input.value,maxSlots));const limit=document.createElement('span');limit.className='slot-limit';setText(limit,controllerAvailable?`node ceiling: ${maxSlots}`:'node controller unavailable');controls.append(input,button,limit);cell.append(status,controls);return cell}
function stageTimeSeries(stage,series){const rows=series?.stages?.[stage]||[];const wrap=document.createElement('div');wrap.className='stage-chart-wrap';const title=document.createElement('div');title.className='stage-chart-title';const label=document.createElement('span');setText(label,'15m outcomes · last 48h');const legend=document.createElement('span');setText(legend,'green success · red failed');title.append(label,legend);wrap.append(title);if(!rows.length){const empty=document.createElement('div');empty.className='chart-empty';setText(empty,'No completed tasks');wrap.append(empty);return wrap}const max=Math.max(1,...rows.map(row=>(row.succeeded||0)+(row.failed||0)));const frame=document.createElement('div');frame.className='chart-frame';const yAxis=document.createElement('div');yAxis.className='chart-y-axis';[max,Math.ceil(max/2),0].forEach(value=>{const tick=document.createElement('span');setText(tick,compactChartCount(value));yAxis.append(tick)});const chart=document.createElement('div');chart.className='chart';chart.dataset.stage=stage;chart.addEventListener('scroll',()=>{if(chart.dataset.restoringScroll!=='true')uiState.chartScroll[stage]=chartScrollSnapshot(chart)},{passive:true});rows.forEach(row=>{const total=(row.succeeded||0)+(row.failed||0),tooltip=`${new Date(row.bucket).toLocaleString()} · success ${row.succeeded||0} · failed ${row.failed||0}`;const bucket=document.createElement('div');bucket.className='bucket';bucket.tabIndex=0;bucket.setAttribute('aria-label',tooltip);bucket.addEventListener('mouseenter',event=>showChartTooltip(event,tooltip));bucket.addEventListener('mousemove',positionChartTooltip);bucket.addEventListener('mouseleave',hideChartTooltip);bucket.addEventListener('focus',event=>showChartTooltip(event,tooltip));bucket.addEventListener('blur',hideChartTooltip);const barSlot=document.createElement('div');barSlot.className='bar-slot';const bar=document.createElement('div');bar.className='bar';bar.style.height=`${Math.max(2,total/max*100)}%`;const success=document.createElement('div');success.className='segment success';success.style.height=`${total?row.succeeded/total*100:0}%`;const failure=document.createElement('div');failure.className='segment failure';failure.style.height=`${total?row.failed/total*100:0}%`;bar.append(success,failure);barSlot.append(bar);const xTick=document.createElement('div');xTick.className='x-tick';const xLabel=document.createElement('span');xLabel.className='x-tick-label';setText(xLabel,compactChartTimestamp(row.bucket));xTick.append(xLabel);bucket.append(barSlot,xTick);chart.append(bucket)});frame.append(yAxis,chart);wrap.append(frame);requestAnimationFrame(()=>updateChartTicks(chart));if(typeof ResizeObserver!=='undefined'){const observer=new ResizeObserver(()=>updateChartTicks(chart));observer.observe(chart);chart._tickObserver=observer}restoreChartScroll(chart,stage);return wrap}
function stageCard(stage,pg,k,maxReplicas,horizontalChart=false){const q=pg.queues?.stages?.[stage]||{},a=pg.activity?.stages?.[stage]||{},w=k.stages?.[stage]||{},t=pg.throughput?.windows?.['300']?.[stage]||{},lifetime=pg.throughput?.lifetime_processed?.[stage]||0;const stale=Number(a.stale||0),leaseNote=` · leased ${q.in_flight||0}${stale?` · stale ${stale}`:''}`;const card=document.createElement('section');card.className=`card stage-card${horizontalChart?' stage-card-horizontal':''}`;card.dataset.stage=stage;const stats=document.createElement('div');stats.className='stage-stats';stats.innerHTML=`<b>${stageNames[stage]}</b><div class="big">${w.pod_phases?.Running||0} Running</div><div class="pod-phases" title="Live k3s pod states. Terminating is a Running pod with a deletion timestamp; Evicted records are retained by the node and never run work."></div><div>desired <b>${w.desired||0}</b> · queue <b>${q.visible||0}</b> · active <b>${a.fresh||0}</b>${leaseNote}</div><div>5m success <b>${t.succeeded||0}</b> (${((t.instances_per_second||0)*60).toFixed(2)}/min)</div><div>lifetime processed <b>${lifetime}</b></div><div>restarts ${w.restarts||0}</div>`;setText(stats.querySelector('.pod-phases'),formatPodPhases(w.pod_phases,w.evicted||0));stats.append(scaleControls(stage,w.desired||0,maxReplicas));card.append(stats,stageTimeSeries(stage,pg.stage_time_series));return card}
function renderStageFlow(pg,k,maxReplicas){const flow=el('stages');flow.replaceChildren();const validationLoop=document.createElement('div');validationLoop.className='validation-loop';validationLoop.setAttribute('aria-label','NOP / Oracle and Repair retry group');const groupTitle=document.createElement('div');groupTitle.className='validation-loop-title';setText(groupTitle,'Validation / repair retry group');validationLoop.append(groupTitle,stageCard('validate',pg,k,maxReplicas,true),stageCard('repair',pg,k,maxReplicas,true));flow.append(stageCard('generate',pg,k,maxReplicas),validationLoop,stageCard('reward',pg,k,maxReplicas),stageCard('push',pg,k,maxReplicas))}
async function submitScale(stage,rawValue,maxReplicas){if(uiState.scaling)return;const feedback=el('scale-feedback');if(!Number.isSafeInteger(maxReplicas)||maxReplicas<0){feedback.className='bad';setText(feedback,'Cluster scaling capacity is unavailable; no change was made.');return}if(!/^\d+$/.test(rawValue)){feedback.className='bad';setText(feedback,`Worker total must be a whole number from 0 to ${maxReplicas}.`);return}const replicas=Number(rawValue);if(!Number.isSafeInteger(replicas)||replicas<0||replicas>maxReplicas){feedback.className='bad';setText(feedback,`Worker total must be between 0 and ${maxReplicas}.`);return}uiState.scaling=true;feedback.className='muted';setText(feedback,`Applying ${stageNames[stage]} total ${replicas}…`);renderButtonsDisabled();try{const response=await fetch('/api/pipeline/scale',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json','X-CSRF-Token':csrfToken},body:JSON.stringify({stage,replicas})});const body=await response.json();if(!response.ok)throw new Error(body.error||`HTTP ${response.status}`);delete uiState.scaleDrafts[stage];feedback.className='ok';setText(feedback,`${stageNames[stage]} configured for ${replicas} workers.`);if(body.status)render(body.status);else await poll()}catch(error){feedback.className='bad';setText(feedback,`Scaling failed: ${error.message}`)}finally{uiState.scaling=false;renderButtonsDisabled()}}
async function submitBuildSlots(node,rawValue,maxSlots){if(uiState.buildSlotUpdating)return;const feedback=el('build-slot-feedback');if(!Number.isSafeInteger(maxSlots)||maxSlots<1){feedback.className='bad';setText(feedback,'Node BuildKit slot capacity is unavailable; no change was made.');return}if(!/^\d+$/.test(rawValue)){feedback.className='bad';setText(feedback,`BuildKit slots must be a whole number from 1 to ${maxSlots}.`);return}const slots=Number(rawValue);if(!Number.isSafeInteger(slots)||slots<1||slots>maxSlots){feedback.className='bad';setText(feedback,`BuildKit slots must be between 1 and ${maxSlots}.`);return}uiState.buildSlotUpdating=true;feedback.className='muted';setText(feedback,`Applying ${slots} local BuildKit slots on ${node}…`);renderButtonsDisabled();try{const response=await fetch('/api/pipeline/build-slots',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json','X-CSRF-Token':csrfToken},body:JSON.stringify({node,slots})});const body=await response.json();if(!response.ok)throw new Error(body.error||`HTTP ${response.status}`);delete uiState.buildSlotDrafts[node];feedback.className='ok';setText(feedback,`${node} configured for ${slots} local BuildKit slots; in-flight builds on retired slots finish normally.`);if(body.status)render(body.status);else await poll()}catch(error){feedback.className='bad';setText(feedback,`BuildKit slot update failed: ${error.message}`)}finally{uiState.buildSlotUpdating=false;renderButtonsDisabled()}}
function renderButtonsDisabled(){document.querySelectorAll('.scale-controls button').forEach(button=>{button.disabled=uiState.scaling||button.dataset.capacityAvailable!=='true'});document.querySelectorAll('.slot-controls button').forEach(button=>{button.disabled=uiState.buildSlotUpdating||button.dataset.controllerAvailable!=='true'})}
function renderResources(metrics,clusterNodes){const status=el('resource-status'),summary=el('resource-summary'),body=el('resource-nodes');summary.replaceChildren();body.replaceChildren();if(!metrics?.available){status.className='bad';setText(status,`Resource metrics unavailable${metrics?.error?`: ${metrics.error}`:''}`)}else if(metrics.stale){status.className='bad';setText(status,`Showing stale metrics from ${metrics.collected_at||'unknown time'}: ${metrics.error||'refresh failed'}`)}else{status.className='muted';setText(status,`Live metrics from ${metrics.collected_at||'—'}`)}const aggregate=metrics?.aggregate||{};[['CPU',cpu(aggregate.cpu_used_millicores),cpu(aggregate.cpu_allocatable_millicores),aggregate.cpu_percent],['Memory',memory(aggregate.memory_used_bytes),memory(aggregate.memory_allocatable_bytes),aggregate.memory_percent]].forEach(([label,used,capacity,percent])=>{const card=document.createElement('div');card.className='card';const title=document.createElement('b');setText(title,`${label} utilization`);const value=document.createElement('div');value.className='big';setText(value,percent==null?'—':`${percent.toFixed(1)}%`);const detail=document.createElement('div');setText(detail,`${used} / ${capacity}`);card.append(title,value,detail);summary.append(card)});const workloadByName=Object.fromEntries((clusterNodes||[]).map(node=>[node.name,node]));(metrics?.nodes||[]).forEach(node=>{const workload=workloadByName[node.name]||{};const tr=document.createElement('tr');const td=document.createElement('td');td.colSpan=3;const details=document.createElement('details');details.className='node-details';details.dataset.nodeKey=node.name;details.open=uiState.expandedNodes.has(node.name);details.addEventListener('toggle',()=>{if(details.open)uiState.expandedNodes.add(node.name);else uiState.expandedNodes.delete(node.name)});const rowSummary=document.createElement('summary');rowSummary.className='node-summary';const identity=document.createElement('span');const stageCounts=Object.entries(workload.pods_by_stage||{}).map(([stage,count])=>`${stageNames[stage]||stage} ${count}`).join(' · ');setText(identity,`${node.name}${node.ip?` / ${node.ip}`:''} — ${workload.pod_count||0} pods${stageCounts?` (${stageCounts})`:''}`);const cpuCell=document.createElement('span');setText(cpuCell,node.available?`${cpu(node.cpu_used_millicores)} / ${cpu(node.cpu_allocatable_millicores)} (${node.cpu_percent?.toFixed(1)??'—'}%)`:'CPU unavailable');const memoryCell=document.createElement('span');setText(memoryCell,node.available?`${memory(node.memory_used_bytes)} / ${memory(node.memory_allocatable_bytes)} (${node.memory_percent?.toFixed(1)??'—'}%)`:'memory unavailable');rowSummary.append(identity,cpuCell,memoryCell);const list=document.createElement('div');list.className='pod-list';if(!(workload.pods||[]).length){const empty=document.createElement('span');empty.className='muted';setText(empty,'No pipeline pods scheduled on this node.');list.append(empty)}else{workload.pods.forEach(pod=>{const podRow=document.createElement('div');podRow.className='pod-row';[pod.name,stageNames[pod.stage]||pod.stage,`${pod.phase}${pod.ready?' / Ready':' / NotReady'}`,`${pod.restarts} restarts`].forEach(value=>{const span=document.createElement('span');setText(span,value);podRow.append(span)});list.append(podRow)})}details.append(rowSummary,list);td.append(details);tr.append(td);body.append(tr)});const resourceScroll=el('resource-scroll');resourceScroll.scrollLeft=uiState.resourceScroll.left;resourceScroll.scrollTop=uiState.resourceScroll.top}
function renderResourcesV2(metrics,clusterNodes){
 const status=el('resource-status'),summary=el('resource-summary'),body=el('resource-nodes');summary.replaceChildren();body.replaceChildren();
 if(!metrics?.available){status.className='bad';setText(status,`Resource metrics unavailable${metrics?.error?`: ${metrics.error}`:''}`)}else if(metrics.stale){status.className='bad';setText(status,`Showing stale metrics from ${metrics.collected_at||'unknown time'}: ${metrics.error||'refresh failed'}`)}else if(metrics.allocation_error){status.className='bad';setText(status,`Live usage metrics; CPU allocation unavailable: ${metrics.allocation_error}`)}else{status.className='muted';setText(status,`Live metrics from ${metrics.collected_at||'—'}`)}
 const aggregate=metrics?.aggregate||{};
 const cpuCard=document.createElement('div');cpuCard.className='card';cpuCard.title='Actual metrics usage / sum of Kubernetes CPU requests / cluster allocatable CPU. Both percentages use allocatable CPU as denominator.';const cpuTitle=document.createElement('b');setText(cpuTitle,'CPU used / allocated / allocatable');const cpuValue=document.createElement('div');cpuValue.className='big';setText(cpuValue,formatCpuTriple(aggregate.cpu_used_millicores,aggregate.cpu_allocated_millicores,aggregate.cpu_allocatable_millicores));cpuCard.append(cpuTitle,cpuValue);summary.append(cpuCard);
 const memoryCard=document.createElement('div');memoryCard.className='card';const memoryTitle=document.createElement('b');setText(memoryTitle,'Memory utilization');const memoryValue=document.createElement('div');memoryValue.className='big';setText(memoryValue,aggregate.memory_percent==null?'—':`${aggregate.memory_percent.toFixed(1)}%`);const memoryDetail=document.createElement('div');setText(memoryDetail,`${memory(aggregate.memory_used_bytes)} / ${memory(aggregate.memory_allocatable_bytes)}`);memoryCard.append(memoryTitle,memoryValue,memoryDetail);summary.append(memoryCard);
 const workloadByName=Object.fromEntries((clusterNodes||[]).map(node=>[node.name,node]));
 (metrics?.nodes||[]).forEach(node=>{const workload=workloadByName[node.name]||{};const tr=document.createElement('tr');const td=document.createElement('td');td.colSpan=5;const details=document.createElement('details');details.className='node-details';details.dataset.nodeKey=node.name;details.open=uiState.expandedNodes.has(node.name);details.addEventListener('toggle',()=>{if(details.open)uiState.expandedNodes.add(node.name);else uiState.expandedNodes.delete(node.name)});const rowSummary=document.createElement('summary');rowSummary.className='node-summary';const identity=document.createElement('span');const stageCounts=Object.entries(workload.pods_by_stage||{}).map(([stage,count])=>`${stageNames[stage]||stage} ${count}`).join(' · ');setText(identity,`${node.name}${node.ip?` / ${node.ip}`:''} — ${workload.pod_count||0} pods${stageCounts?` (${stageCounts})`:''}`);const cpuCell=document.createElement('span');cpuCell.title='Actual metrics usage / sum of Kubernetes CPU requests / node allocatable CPU. Both percentages use allocatable CPU as denominator.';setText(cpuCell,formatCpuTriple(node.cpu_used_millicores,node.cpu_allocated_millicores,node.cpu_allocatable_millicores));const memoryCell=document.createElement('span');setText(memoryCell,node.available?`${memory(node.memory_used_bytes)} / ${memory(node.memory_allocatable_bytes)} (${node.memory_percent?.toFixed(1)??'—'}%)`:'memory unavailable');const diskCell=document.createElement('span');diskCell.className=(node.disk_io?.busy_percent??0)>=80?'bad':'';diskCell.title=node.disk_io?.error||'30-second cAdvisor rate sample';setText(diskCell,formatDiskIo(node.disk_io));const slotCell=buildSlotControls(workload,node.build_slots);rowSummary.append(identity,cpuCell,memoryCell,diskCell,slotCell);const list=document.createElement('div');list.className='pod-list';if(!(workload.pods||[]).length){const empty=document.createElement('span');empty.className='muted';setText(empty,'No pipeline pods scheduled on this node.');list.append(empty)}else{workload.pods.forEach(pod=>{const podRow=document.createElement('div');podRow.className='pod-row';[pod.name,stageNames[pod.stage]||pod.stage,`${pod.phase}${pod.ready?' / Ready':' / NotReady'}`,`${pod.restarts} restarts`].forEach(value=>{const span=document.createElement('span');setText(span,value);podRow.append(span)});list.append(podRow)})}details.append(rowSummary,list);td.append(details);tr.append(td);body.append(tr)});
 const resourceScroll=el('resource-scroll');resourceScroll.scrollLeft=uiState.resourceScroll.left;resourceScroll.scrollTop=uiState.resourceScroll.top;
}
function renderStorage(storage){const warning=el('storage-warning'),summary=el('storage-summary'),mounts=el('storage-mounts');warning.replaceChildren();summary.replaceChildren();mounts.replaceChildren();if(storage?.warning){warning.className='warning';setText(warning,`⚠ ${storage.warning}`)}else{warning.className='';setText(warning,'')}[["Runtime workspace root",storage?.workspace_root||'unknown'],["Durable source of truth",storage?.source_of_truth||'unknown']].forEach(([label,value])=>{const card=document.createElement('div');card.className='card';const title=document.createElement('b');setText(title,label);const path=document.createElement('code');path.className='path';setText(path,value);card.append(title,path);summary.append(card)});(storage?.mounts||[]).forEach(mount=>{const tr=document.createElement('tr');[`${mount.deployment||'—'} / ${stageNames[mount.stage]||mount.stage||'—'}`,mount.node_ip||'unspecified',mount.mount_path||'—',mount.source_path||mount.kind||'—',mount.durability||'unknown'].forEach(value=>{const td=document.createElement('td');const code=document.createElement('code');code.className='path';setText(code,value);td.append(code);tr.append(td)});mounts.append(tr)})}
function farmCard(summary,label,value,detail){const card=document.createElement('div');card.className='card';const title=document.createElement('b');setText(title,label);const main=document.createElement('div');main.className='big';setText(main,value);const note=document.createElement('div');note.className='farm-detail';setText(note,detail);card.append(title,main,note);summary.append(card)}
function renderRemoteBuildKit(farm,tracking){const status=el('buildkit-farm-status'),warning=el('buildkit-farm-warning'),summary=el('buildkit-farm-summary'),diskBody=el('buildkit-farm-disk-io');warning.replaceChildren();summary.replaceChildren();diskBody.replaceChildren();const gateway=farm?.gateway||{},ready=farm?.ready||{},resources=farm?.resources||{};const freshness=resources.checked_at||ready.checked_at||gateway.checked_at;status.className=gateway.ok&&ready.ok?'farm-status muted':'farm-status bad';setText(status,`${farm?.sampling?'Sampling farm; ':''}gateway ${gateway.status||'unknown'} · readiness ${ready.status||'unknown'} · ${freshness?`sampled ${freshness}`:'awaiting first sample'} · ${farm?.poll_interval_seconds||30}s minimum poll`);if(resources.schema_warning||resources.error){warning.className='warning';setText(warning,resources.error||resources.schema_warning)}else{warning.className='';setText(warning,'')}farmCard(summary,'Gateway / ready',`${gateway.ok?'up':'down'} / ${ready.ok?'ready':'not ready'}`,`HTTP ${gateway.http_status??'—'} / ${ready.http_status??'—'}`);const available=resources.available_backend_count==null?'—':resources.available_backend_count;const sampled=resources.backend_count??resources.sampled_worker_count??0;farmCard(summary,'Backend workers',`${sampled} sampled / ${available} available`,resources.sampled_worker?`latest ${resources.sampled_worker} · ${resources.scope||'unknown scope'}`:(resources.scope||'unknown scope'));const queueLabel=resources.is_global?'Farm queue':'Sampled worker queue';farmCard(summary,queueLabel,`${resources.queue_length??'—'} / ${resources.queue_capacity??'—'}`,resources.is_global?'global aggregate':(resources.sampled_worker||'worker-local sample'));farmCard(summary,'Active builds',`${resources.running_builds??'—'} running`,`${resources.inflight_builds??'—'} inflight · ${resources.is_global?'global':'sampled/local'}`);const counts=tracking?.status_counts||{};const breakdown=Object.entries(counts).map(([name,count])=>`${name} ${count}`).join(' · ');farmCard(summary,'SWEgen remote pending',tracking?.pending??'—',tracking?.available?(breakdown||'no remote submissions recorded'):'router tracking table unavailable');const diskRows=resources.node_disk_io||[];if(!diskRows.length){const tr=document.createElement('tr');const td=document.createElement('td');td.colSpan=4;setText(td,'Remote per-node disk I/O telemetry is not exposed by the current worker-local API sample.');tr.append(td);diskBody.append(tr)}else{diskRows.forEach(row=>{const tr=document.createElement('tr');const busy=row.busy_percent==null?'—':`${row.busy_percent.toFixed(1)}%`;[[row.node||'unknown'],[`${rateBytes(row.read_bytes_per_second)} / ${rateBytes(row.write_bytes_per_second)}`],[`${rateOps(row.read_iops)} / ${rateOps(row.write_iops)}`],[`${busy} / ${row.io_current??'—'} inflight`]].forEach(([value])=>{const td=document.createElement('td');setText(td,value);tr.append(td)});diskBody.append(tr)})}}
function renderHourlyYield(series){el('yield').replaceChildren();const source=series?.stages||{};stages.forEach(stage=>{const rows=source[stage]||[];const card=document.createElement('div');card.className='card';const title=document.createElement('b');setText(title,stageNames[stage]);card.append(title);const list=document.createElement('div');list.className='yield-list';if(!rows.length){const empty=document.createElement('div');empty.className='muted';setText(empty,'No terminal outcomes in the last 12 hours');list.append(empty)}else{rows.forEach(row=>{const line=document.createElement('div');line.className='yield-row';const stamp=document.createElement('span');setText(stamp,new Date(row.bucket).toLocaleString([],{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}));const value=document.createElement('span');value.className='yield-value';setText(value,`${row.succeeded} / ${row.processed}`);const percent=document.createElement('span');percent.className='yield-percent';setText(percent,row.yield_percent==null?'—':`${row.yield_percent.toFixed(1)}%`);line.append(stamp,value,percent);list.append(line)})}card.append(list);el('yield').append(card)})}
function render(data){captureUiState();el('stamp').textContent=`Updated ${data.generated_at||'—'} · refreshes every 5s`; el('errors').replaceChildren();
 Object.entries(data.sources||{}).forEach(([n,s])=>{if(!s.ok){const d=document.createElement('div');d.className='banner bad';setText(d,`${n} unavailable: ${s.error}`);el('errors').append(d)}});
 const pg=data.postgres||{}, k=data.k3s||{},maxReplicas=k.scaling?.max_replicas;renderStageFlow(pg,k,maxReplicas);
 renderResourcesV2(k.resource_metrics,k.nodes);
 renderStorage(k.storage);
 renderRemoteBuildKit(data.buildkit_farm||{},pg.remote_builds||{});
 renderHourlyYield(pg.hourly_yield);
 el('tasks').replaceChildren();(pg.tasks||[]).forEach(task=>{const taskKey=`${task.task_id}:${task.task_version}`;const tr=document.createElement('tr');const timeline=(task.stages||[]).map(s=>`${s.stage}: ${s.state} wait ${secs(s.wait_seconds)} run ${secs(s.run_seconds)}${s.worker_id?' @ '+s.worker_id:''}`).join('\n');[task.task_id,task.state,task.current_stage,secs(task.total_elapsed_seconds)].forEach(v=>{const td=document.createElement('td');setText(td,v);tr.append(td)});const storage=task.storage||{};const storageCell=document.createElement('td');const runtimePath=document.createElement('code');runtimePath.className='path';setText(runtimePath,storage.runtime_path_pattern||'No runtime path recorded');const storageNote=document.createElement('div');storageNote.className='storage-note';setText(storageNote,`${storage.generated_on_node?`node ${storage.generated_on_node} · `:''}${storage.runtime_directory_state||'unknown lifecycle'} · PostgreSQL: ${storage.stored_file_count||0} files / ${bytes(storage.stored_bytes||0)}`);storageCell.append(runtimePath,storageNote);tr.append(storageCell);const td=document.createElement('td');const details=document.createElement('details');details.dataset.taskKey=taskKey;details.open=uiState.expandedTasks.has(taskKey);details.addEventListener('toggle',()=>{if(details.open)uiState.expandedTasks.add(taskKey);else uiState.expandedTasks.delete(taskKey)});const detailsSummary=document.createElement('summary');setText(detailsSummary,'show');const pre=document.createElement('pre');setText(pre,timeline);details.append(detailsSummary,pre);td.append(details);tr.append(td);el('tasks').append(tr)});
}
async function poll(){try{const r=await fetch('/api/pipeline/status',{cache:'no-store'});if(!r.ok)throw Error(`HTTP ${r.status}`);render(await r.json())}catch(e){el('stamp').textContent=`Dashboard fetch failed: ${e}`}}
poll();setInterval(poll,5000);
</script></main></body></html>"""


def make_handler(
    cache: SnapshotCache,
    scaler: K3sScaler,
    build_slot_controller: K3sBuildSlotController,
    csrf_token: str,
) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/":
                body = HTML.replace("__CSRF_TOKEN__", csrf_token).encode()
                self._send(200, "text/html; charset=utf-8", body)
            elif self.path == "/api/pipeline/status":
                body = json.dumps(cache.snapshot()).encode()
                self._send(200, "application/json", body)
            elif self.path == "/healthz":
                self._send(200, "application/json", b'{"ok":true}')
            else:
                self._send(404, "application/json", b'{"error":"not found"}')

        def do_POST(self) -> None:
            if self.path not in {
                "/api/pipeline/scale",
                "/api/pipeline/build-slots",
            }:
                self._send_json(404, {"error": "not found"})
                return
            if not self._request_is_same_origin(csrf_token):
                self._send_json(403, {"error": "invalid origin or CSRF token"})
                return
            if self.headers.get_content_type() != "application/json":
                self._send_json(415, {"error": "Content-Type must be application/json"})
                return
            try:
                content_length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self._send_json(400, {"error": "invalid Content-Length"})
                return
            if not 0 < content_length <= 1024:
                self._send_json(400, {"error": "request body must be between 1 and 1024 bytes"})
                return
            try:
                payload = json.loads(self.rfile.read(content_length))
                if not isinstance(payload, dict):
                    raise ValueError("request body must be a JSON object")
                current_status = cache.snapshot()
                if self.path == "/api/pipeline/scale":
                    stage = payload.get("stage")
                    replicas = payload.get("replicas")
                    max_replicas = (
                        current_status.get("k3s", {}).get("scaling", {}).get("max_replicas")
                    )
                    applied: object = scaler.scale(
                        stage,
                        replicas,
                        max_replicas=max_replicas,
                    )
                    response_fields = {
                        "stage": stage,
                        "replicas": replicas,
                        "applied": applied,
                    }
                else:
                    node = payload.get("node")
                    slots = payload.get("slots")
                    applied = build_slot_controller.update(
                        node,
                        slots,
                        nodes=current_status.get("k3s", {}).get("nodes"),
                    )
                    response_fields = {
                        "node": node,
                        "slots": slots,
                        "applied": applied,
                    }
            except (BuildSlotBusyError, ScalingBusyError) as error:
                self._send_json(409, {"error": str(error)})
                return
            except (json.JSONDecodeError, ValueError) as error:
                self._send_json(400, {"error": str(error)})
                return
            except Exception as error:
                self._send_json(502, {"error": f"control request failed: {str(error)[:500]}"})
                return
            cache.refresh()
            self._send_json(
                200,
                {
                    "ok": True,
                    **response_fields,
                    "status": cache.snapshot(),
                },
            )

        def _request_is_same_origin(self, expected_token: str) -> bool:
            supplied_token = self.headers.get("X-CSRF-Token", "")
            if not hmac.compare_digest(supplied_token, expected_token):
                return False
            origin = self.headers.get("Origin", "")
            host = self.headers.get("Host", "")
            if not origin or not host:
                return False
            parsed = urlsplit(origin)
            return parsed.scheme in {"http", "https"} and parsed.netloc == host

        def _send_json(self, status: int, value: dict[str, Any]) -> None:
            self._send(status, "application/json", json.dumps(value).encode())

        def _send(self, status: int, content_type: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def serve(host: str = "0.0.0.0", port: int = 8766) -> None:
    cache = SnapshotCache()
    scaler = K3sScaler()
    build_slot_controller = K3sBuildSlotController()
    csrf_token = secrets.token_urlsafe(32)
    cache.refresh()
    thread = threading.Thread(target=cache.run, name="dashboard-refresh", daemon=True)
    thread.start()
    server = ThreadingHTTPServer(
        (host, port),
        make_handler(cache, scaler, build_slot_controller, csrf_token),
    )
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        cache.stop()
        server.server_close()
        thread.join(timeout=2)
