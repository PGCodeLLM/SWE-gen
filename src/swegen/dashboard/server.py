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

from swegen.dashboard.distributed_status import K3sStatusCollector, PipelineStatusCollector

SCALE_MIN = 0
GENERATE_MAIN_CAPACITY = 92
PRIMARY_DEPLOYMENTS = {
    "validate": "swegen-validate",
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


class SnapshotCache:
    def __init__(self, *, refresh_seconds: float = 5.0) -> None:
        self.refresh_seconds = refresh_seconds
        self._lock = threading.Lock()
        self._stopped = threading.Event()
        self._snapshot: dict[str, Any] = {"postgres": {}, "k3s": {}, "sources": {}}
        self._collectors = {
            "postgres": PipelineStatusCollector(),
            "k3s": K3sStatusCollector(),
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
main{max-width:1500px;margin:auto;padding:24px}h1{font-size:24px;margin:0 0 4px}.muted{color:var(--muted)}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:12px;margin:16px 0}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px}.big{font-size:27px;font-weight:700}
.ok{color:var(--ok)}.bad{color:var(--bad)}table{width:100%;border-collapse:collapse;background:var(--card)}
th,td{text-align:left;padding:9px;border-bottom:1px solid var(--line);vertical-align:top}th{color:var(--muted)}
.scroll{overflow:auto;border:1px solid var(--line);border-radius:10px}details summary{cursor:pointer}.banner{padding:10px;border:1px solid var(--bad);border-radius:8px;margin:8px 0}
.legend{display:flex;gap:18px;margin:8px 0;color:var(--muted)}.swatch{display:inline-block;width:11px;height:11px;border-radius:2px;margin-right:5px}.success{background:#32d583}.failure{background:#ff6b6b}
.chart-card{min-width:0}.chart{height:220px;display:flex;align-items:flex-end;gap:3px;border-bottom:1px solid var(--line);padding:8px 2px 0;overflow-x:auto}.bucket{height:100%;min-width:25px;flex:1;display:flex;flex-direction:column;justify-content:flex-end}.bar{display:flex;flex-direction:column-reverse;justify-content:flex-start;min-height:1px}.segment{width:100%;min-height:0}.time{font-size:9px;color:var(--muted);height:42px;writing-mode:vertical-rl;transform:rotate(180deg);margin:4px auto 0}.empty{height:220px;display:grid;place-items:center;color:var(--muted);border-bottom:1px solid var(--line)}
.yield-list{margin-top:10px}.yield-row{display:grid;grid-template-columns:1fr auto auto;gap:10px;padding:7px 0;border-bottom:1px solid var(--line)}.yield-row:last-child{border-bottom:0}.yield-value{font-variant-numeric:tabular-nums}.yield-percent{min-width:52px;text-align:right;font-weight:700}
.scale-controls{display:grid;grid-template-columns:90px 1fr;gap:8px;margin-top:12px}.scale-controls input,.scale-controls button{border:1px solid var(--line);border-radius:6px;padding:8px;background:#09182b;color:#edf4ff}.scale-controls button{cursor:pointer;background:#174b78}.scale-controls button:disabled{cursor:wait;opacity:.55}.scale-limit{grid-column:1/-1;color:var(--muted);font-size:11px}#scale-feedback{min-height:20px;margin-top:8px}
.warning{padding:14px;border:2px solid #f79009;background:#3b2605;color:#ffd79a;border-radius:10px;margin:12px 0}.path{display:block;max-width:520px;overflow-wrap:anywhere;font:12px ui-monospace,SFMono-Regular,Consolas,monospace;color:#b8d8ff}.storage-note{margin-top:5px;color:var(--muted);font-size:12px}
</style></head><body><main><h1>SWE-gen k3s + PGMQ</h1><div id="stamp" class="muted"></div><div id="errors"></div>
<h2>Stages</h2><div id="scale-feedback" class="muted"></div><div id="stages" class="grid"></div><h2>Nodes</h2><div id="nodes" class="grid"></div>
<h2>Cluster resources</h2><div id="resource-status" class="muted"></div><div id="resource-summary" class="grid"></div><div class="scroll"><table><thead><tr><th>Node / IP</th><th>CPU used / allocatable</th><th>Memory used / allocatable</th></tr></thead><tbody id="resource-nodes"></tbody></table></div>
<h2>Harbor task storage</h2><div id="storage-warning"></div><div id="storage-summary" class="grid"></div><div class="scroll"><table><thead><tr><th>Deployment / stage</th><th>Node</th><th>Container path</th><th>Backing storage</th><th>Durability</th></tr></thead><tbody id="storage-mounts"></tbody></table></div>
<h2>15-minute outcomes</h2><div class="legend"><span><i class="swatch success"></i>success</span><span><i class="swatch failure"></i>failed/rejected</span></div><div id="timeline" class="grid"></div>
<h2>Hourly yield</h2><div class="muted">Success / all terminal outcomes (success + failed/rejected)</div><div id="yield" class="grid"></div>
<h2>Task states</h2><div id="states" class="grid"></div><h2>Recent tasks</h2><div class="scroll"><table><thead><tr><th>Task</th><th>State</th><th>Stage</th><th>Elapsed</th><th>Task directory / durable source</th><th>Timeline</th></tr></thead><tbody id="tasks"></tbody></table></div>
<script>
const stages=['generate','validate','reward','push']; const el=id=>document.getElementById(id);
const stageNames={generate:'SWEgen',validate:'NOP / Oracle',reward:'Reward hack',push:'SWR push'};
const csrfToken=document.querySelector('meta[name="csrf-token"]').content;
const secs=n=>n==null?'—':n<60?`${Math.round(n)}s`:n<3600?`${(n/60).toFixed(1)}m`:`${(n/3600).toFixed(1)}h`;
const cpu=m=>m==null?'—':`${(m/1000).toFixed(2)} cores`;
const memory=b=>b==null?'—':`${(b/1024/1024/1024).toFixed(1)} GiB`;
const bytes=b=>b==null?'—':b<1024*1024?`${(b/1024).toFixed(1)} KiB`:`${(b/1024/1024).toFixed(1)} MiB`;
function setText(node,value){node.textContent=value==null?'—':String(value)}
const uiState={chartScroll:{},expandedTasks:new Set(),scaleDrafts:{},scaling:false};
function captureUiState(){document.querySelectorAll('.chart[data-stage]').forEach(chart=>{uiState.chartScroll[chart.dataset.stage]=chart.scrollLeft});document.querySelectorAll('#tasks details[data-task-key]').forEach(details=>{if(details.open)uiState.expandedTasks.add(details.dataset.taskKey);else uiState.expandedTasks.delete(details.dataset.taskKey)})}
function scaleControls(stage,desired,maxReplicas){const capacityAvailable=Number.isSafeInteger(maxReplicas)&&maxReplicas>=0;const controls=document.createElement('div');controls.className='scale-controls';const input=document.createElement('input');input.type='number';input.min='0';input.max=capacityAvailable?String(maxReplicas):String(desired||0);input.step='1';input.value=uiState.scaleDrafts[stage]??desired??0;input.setAttribute('aria-label',`${stageNames[stage]} total workers`);input.addEventListener('input',()=>{uiState.scaleDrafts[stage]=input.value});const button=document.createElement('button');button.type='button';button.dataset.capacityAvailable=String(capacityAvailable);button.disabled=uiState.scaling||!capacityAvailable;setText(button,'Apply configuration');button.addEventListener('click',()=>submitScale(stage,input.value,maxReplicas));const limit=document.createElement('span');limit.className='scale-limit';setText(limit,capacityAvailable?`cluster CPU ceiling: ${maxReplicas}`:'cluster CPU ceiling unavailable');controls.append(input,button,limit);return controls}
async function submitScale(stage,rawValue,maxReplicas){if(uiState.scaling)return;const feedback=el('scale-feedback');if(!Number.isSafeInteger(maxReplicas)||maxReplicas<0){feedback.className='bad';setText(feedback,'Cluster scaling capacity is unavailable; no change was made.');return}if(!/^\d+$/.test(rawValue)){feedback.className='bad';setText(feedback,`Worker total must be a whole number from 0 to ${maxReplicas}.`);return}const replicas=Number(rawValue);if(!Number.isSafeInteger(replicas)||replicas<0||replicas>maxReplicas){feedback.className='bad';setText(feedback,`Worker total must be between 0 and ${maxReplicas}.`);return}uiState.scaling=true;feedback.className='muted';setText(feedback,`Applying ${stageNames[stage]} total ${replicas}…`);renderButtonsDisabled();try{const response=await fetch('/api/pipeline/scale',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json','X-CSRF-Token':csrfToken},body:JSON.stringify({stage,replicas})});const body=await response.json();if(!response.ok)throw new Error(body.error||`HTTP ${response.status}`);delete uiState.scaleDrafts[stage];feedback.className='ok';setText(feedback,`${stageNames[stage]} configured for ${replicas} workers.`);if(body.status)render(body.status);else await poll()}catch(error){feedback.className='bad';setText(feedback,`Scaling failed: ${error.message}`)}finally{uiState.scaling=false;renderButtonsDisabled()}}
function renderButtonsDisabled(){document.querySelectorAll('.scale-controls button').forEach(button=>{button.disabled=uiState.scaling||button.dataset.capacityAvailable!=='true'})}
function renderResources(metrics){const status=el('resource-status'),summary=el('resource-summary'),body=el('resource-nodes');summary.replaceChildren();body.replaceChildren();if(!metrics?.available){status.className='bad';setText(status,`Resource metrics unavailable${metrics?.error?`: ${metrics.error}`:''}`)}else if(metrics.stale){status.className='bad';setText(status,`Showing stale metrics from ${metrics.collected_at||'unknown time'}: ${metrics.error||'refresh failed'}`)}else{status.className='muted';setText(status,`Live metrics from ${metrics.collected_at||'—'}`)}const aggregate=metrics?.aggregate||{};[['CPU',cpu(aggregate.cpu_used_millicores),cpu(aggregate.cpu_allocatable_millicores),aggregate.cpu_percent],['Memory',memory(aggregate.memory_used_bytes),memory(aggregate.memory_allocatable_bytes),aggregate.memory_percent]].forEach(([label,used,capacity,percent])=>{const card=document.createElement('div');card.className='card';const title=document.createElement('b');setText(title,`${label} utilization`);const value=document.createElement('div');value.className='big';setText(value,percent==null?'—':`${percent.toFixed(1)}%`);const detail=document.createElement('div');setText(detail,`${used} / ${capacity}`);card.append(title,value,detail);summary.append(card)});(metrics?.nodes||[]).forEach(node=>{const tr=document.createElement('tr');const identity=document.createElement('td');setText(identity,`${node.name}${node.ip?` / ${node.ip}`:''}`);const cpuCell=document.createElement('td');setText(cpuCell,node.available?`${cpu(node.cpu_used_millicores)} / ${cpu(node.cpu_allocatable_millicores)} (${node.cpu_percent?.toFixed(1)??'—'}%)`:'unavailable');const memoryCell=document.createElement('td');setText(memoryCell,node.available?`${memory(node.memory_used_bytes)} / ${memory(node.memory_allocatable_bytes)} (${node.memory_percent?.toFixed(1)??'—'}%)`:'unavailable');tr.append(identity,cpuCell,memoryCell);body.append(tr)})}
function renderStorage(storage){const warning=el('storage-warning'),summary=el('storage-summary'),mounts=el('storage-mounts');warning.replaceChildren();summary.replaceChildren();mounts.replaceChildren();if(storage?.warning){warning.className='warning';setText(warning,`⚠ ${storage.warning}`)}else{warning.className='';setText(warning,'')}[["Runtime workspace root",storage?.workspace_root||'unknown'],["Durable source of truth",storage?.source_of_truth||'unknown']].forEach(([label,value])=>{const card=document.createElement('div');card.className='card';const title=document.createElement('b');setText(title,label);const path=document.createElement('code');path.className='path';setText(path,value);card.append(title,path);summary.append(card)});(storage?.mounts||[]).forEach(mount=>{const tr=document.createElement('tr');[`${mount.deployment||'—'} / ${stageNames[mount.stage]||mount.stage||'—'}`,mount.node_ip||'unspecified',mount.mount_path||'—',mount.source_path||mount.kind||'—',mount.durability||'unknown'].forEach(value=>{const td=document.createElement('td');const code=document.createElement('code');code.className='path';setText(code,value);td.append(code);tr.append(td)});mounts.append(tr)})}
function renderTimeSeries(series){el('timeline').replaceChildren();const source=series?.stages||{};stages.forEach(stage=>{const rows=source[stage]||[];const card=document.createElement('div');card.className='card chart-card';const title=document.createElement('b');setText(title,stageNames[stage]);card.append(title);if(!rows.length){const empty=document.createElement('div');empty.className='empty';setText(empty,'No completed tasks in the last 6 hours');card.append(empty);el('timeline').append(card);return}const max=Math.max(1,...rows.map(r=>(r.succeeded||0)+(r.failed||0)));const chart=document.createElement('div');chart.className='chart';chart.dataset.stage=stage;chart.addEventListener('scroll',()=>{uiState.chartScroll[stage]=chart.scrollLeft},{passive:true});rows.forEach(row=>{const total=(row.succeeded||0)+(row.failed||0);const bucket=document.createElement('div');bucket.className='bucket';bucket.title=`${new Date(row.bucket).toLocaleString()} · success ${row.succeeded||0} · failed ${row.failed||0}`;const bar=document.createElement('div');bar.className='bar';bar.style.height=`${Math.max(2,total/max*165)}px`;const success=document.createElement('div');success.className='segment success';success.style.height=`${total?row.succeeded/total*100:0}%`;const failure=document.createElement('div');failure.className='segment failure';failure.style.height=`${total?row.failed/total*100:0}%`;bar.append(success,failure);const label=document.createElement('span');label.className='time';setText(label,new Date(row.bucket).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'}));bucket.append(bar,label);chart.append(bucket)});card.append(chart);el('timeline').append(card);chart.scrollLeft=uiState.chartScroll[stage]||0})}
function renderHourlyYield(series){el('yield').replaceChildren();const source=series?.stages||{};stages.forEach(stage=>{const rows=source[stage]||[];const card=document.createElement('div');card.className='card';const title=document.createElement('b');setText(title,stageNames[stage]);card.append(title);const list=document.createElement('div');list.className='yield-list';if(!rows.length){const empty=document.createElement('div');empty.className='muted';setText(empty,'No terminal outcomes in the last 12 hours');list.append(empty)}else{rows.forEach(row=>{const line=document.createElement('div');line.className='yield-row';const stamp=document.createElement('span');setText(stamp,new Date(row.bucket).toLocaleString([],{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}));const value=document.createElement('span');value.className='yield-value';setText(value,`${row.succeeded} / ${row.processed}`);const percent=document.createElement('span');percent.className='yield-percent';setText(percent,row.yield_percent==null?'—':`${row.yield_percent.toFixed(1)}%`);line.append(stamp,value,percent);list.append(line)})}card.append(list);el('yield').append(card)})}
function render(data){captureUiState();el('stamp').textContent=`Updated ${data.generated_at||'—'} · refreshes every 5s`; el('errors').replaceChildren();
 Object.entries(data.sources||{}).forEach(([n,s])=>{if(!s.ok){const d=document.createElement('div');d.className='banner bad';setText(d,`${n} unavailable: ${s.error}`);el('errors').append(d)}});
 const pg=data.postgres||{}, k=data.k3s||{},maxReplicas=k.scaling?.max_replicas; el('stages').replaceChildren(); stages.forEach(stage=>{const q=pg.queues?.stages?.[stage]||{},w=k.stages?.[stage]||{},t=pg.throughput?.windows?.['300']?.[stage]||{};const d=document.createElement('div');d.className='card';d.innerHTML=`<b>${stage.toUpperCase()}</b><div class="big">${w.ready||0}/${w.desired||0} ready</div><div>queue <b>${q.visible||0}</b> · active <b>${q.in_flight||0}</b></div><div>5m success <b>${t.succeeded||0}</b> (${((t.instances_per_second||0)*60).toFixed(2)}/min)</div><div>restarts ${w.restarts||0}</div>`;d.append(scaleControls(stage,w.desired||0,maxReplicas));el('stages').append(d)});
 const dead=pg.queues?.dead||{};const dd=document.createElement('div');dd.className='card';dd.innerHTML=`<b>DEAD LETTERS</b><div class="big ${dead.length?'bad':'ok'}">${dead.length||0}</div>`;el('stages').append(dd);
 el('nodes').replaceChildren();(k.nodes||[]).forEach(n=>{const d=document.createElement('div');d.className='card';d.innerHTML=`<b>${n.name}</b><div class="big ${n.ready?'ok':'bad'}">${n.ready?'Ready':'Not Ready'}</div><div>${(n.pressure||[]).join(', ')||'No pressure'}</div>`;el('nodes').append(d)});
 renderResources(k.resource_metrics);
 renderStorage(k.storage);
 renderTimeSeries(pg.stage_time_series);
 renderHourlyYield(pg.hourly_yield);
 el('states').replaceChildren();Object.entries(pg.task_counts?.by_state||{}).forEach(([state,count])=>{const d=document.createElement('div');d.className='card';d.innerHTML=`<b>${state}</b><div class="big">${count}</div>`;el('states').append(d)});
 el('tasks').replaceChildren();(pg.tasks||[]).forEach(task=>{const taskKey=`${task.task_id}:${task.task_version}`;const tr=document.createElement('tr');const timeline=(task.stages||[]).map(s=>`${s.stage}: ${s.state} wait ${secs(s.wait_seconds)} run ${secs(s.run_seconds)}${s.worker_id?' @ '+s.worker_id:''}`).join('\n');[task.task_id,task.state,task.current_stage,secs(task.total_elapsed_seconds)].forEach(v=>{const td=document.createElement('td');setText(td,v);tr.append(td)});const storage=task.storage||{};const storageCell=document.createElement('td');const runtimePath=document.createElement('code');runtimePath.className='path';setText(runtimePath,storage.runtime_path_pattern||'No runtime path recorded');const storageNote=document.createElement('div');storageNote.className='storage-note';setText(storageNote,`${storage.generated_on_node?`node ${storage.generated_on_node} · `:''}${storage.runtime_directory_state||'unknown lifecycle'} · PostgreSQL: ${storage.stored_file_count||0} files / ${bytes(storage.stored_bytes||0)}`);storageCell.append(runtimePath,storageNote);tr.append(storageCell);const td=document.createElement('td');const details=document.createElement('details');details.dataset.taskKey=taskKey;details.open=uiState.expandedTasks.has(taskKey);details.addEventListener('toggle',()=>{if(details.open)uiState.expandedTasks.add(taskKey);else uiState.expandedTasks.delete(taskKey)});const detailsSummary=document.createElement('summary');setText(detailsSummary,'show');const pre=document.createElement('pre');setText(pre,timeline);details.append(detailsSummary,pre);td.append(details);tr.append(td);el('tasks').append(tr)});
}
async function poll(){try{const r=await fetch('/api/pipeline/status',{cache:'no-store'});if(!r.ok)throw Error(`HTTP ${r.status}`);render(await r.json())}catch(e){el('stamp').textContent=`Dashboard fetch failed: ${e}`}}
poll();setInterval(poll,5000);
</script></main></body></html>"""


def make_handler(
    cache: SnapshotCache,
    scaler: K3sScaler,
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
            if self.path != "/api/pipeline/scale":
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
                stage = payload.get("stage")
                replicas = payload.get("replicas")
                current_status = cache.snapshot()
                max_replicas = current_status.get("k3s", {}).get("scaling", {}).get("max_replicas")
                applied = scaler.scale(
                    stage,
                    replicas,
                    max_replicas=max_replicas,
                )
            except ScalingBusyError as error:
                self._send_json(409, {"error": str(error)})
                return
            except (json.JSONDecodeError, ValueError) as error:
                self._send_json(400, {"error": str(error)})
                return
            except Exception as error:
                self._send_json(502, {"error": f"scaling failed: {str(error)[:500]}"})
                return
            cache.refresh()
            self._send_json(
                200,
                {
                    "ok": True,
                    "stage": stage,
                    "replicas": replicas,
                    "applied": applied,
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
    csrf_token = secrets.token_urlsafe(32)
    cache.refresh()
    thread = threading.Thread(target=cache.run, name="dashboard-refresh", daemon=True)
    thread.start()
    server = ThreadingHTTPServer((host, port), make_handler(cache, scaler, csrf_token))
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        cache.stop()
        server.server_close()
        thread.join(timeout=2)
