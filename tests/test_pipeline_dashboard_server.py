from __future__ import annotations

import json
import re
from subprocess import CompletedProcess, run

import pytest


def evaluate_chart_scroll_target(saved: dict[str, object] | None, max_scroll: int) -> int:
    from swegen.dashboard.server import HTML

    match = re.search(
        r"function chartScrollTarget\(saved,maxScroll\)\{[^}]+\}",
        HTML,
    )
    assert match is not None
    saved_javascript = "undefined" if saved is None else json.dumps(saved)
    completed = run(
        [
            "node",
            "-e",
            f"{match.group(0)};process.stdout.write(String("
            f"chartScrollTarget({saved_javascript},{max_scroll})));",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return int(completed.stdout)


def evaluate_chart_scroll_snapshot(
    scroll_left: int,
    scroll_width: int,
    client_width: int,
) -> dict[str, object]:
    from swegen.dashboard.server import HTML

    definition = (
        "const chartScrollSnapshot="
        + HTML.split("const chartScrollSnapshot=", 1)[1].split("function setText", 1)[0]
    )
    completed = run(
        [
            "node",
            "-e",
            definition
            + "process.stdout.write(JSON.stringify(chartScrollSnapshot("
            + f"{{scrollLeft:{scroll_left},scrollWidth:{scroll_width},"
            + f"clientWidth:{client_width}}})));",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def evaluate_chart_restore_sequence(saved: dict[str, object] | None) -> dict[str, object]:
    from swegen.dashboard.server import HTML

    target = re.search(
        r"function chartScrollTarget\(saved,maxScroll\)\{[^}]+\}",
        HTML,
    )
    assert target is not None
    restore = (
        "function restoreChartScroll"
        + HTML.split("function restoreChartScroll", 1)[1].split("function positionChartTooltip", 1)[
            0
        ]
    )
    saved_javascript = "{}" if saved is None else json.dumps({"validate": saved})
    script = f"""
const uiState={{chartScroll:{saved_javascript}}};
const callbacks=[];
const requestAnimationFrame=callback=>callbacks.push(callback);
const chart={{dataset:{{stage:'validate'}},scrollWidth:200,clientWidth:200,scrollLeft:0}};
{target.group(0)}
{restore}
restoreChartScroll(chart,'validate');
callbacks.shift()();
chart.scrollWidth=617;
callbacks.shift()();
chart.scrollWidth=705;
callbacks.shift()();
process.stdout.write(JSON.stringify({{left:chart.scrollLeft,state:uiState.chartScroll.validate,restoring:chart.dataset.restoringScroll||null}}));
"""
    completed = run(
        ["node", "-e", script],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def evaluate_cpu_triple(
    used: str,
    allocated: str,
    allocatable: str,
) -> str:
    from swegen.dashboard.server import HTML

    definitions = []
    for name in ("compactCores", "cpuPart", "formatCpuTriple"):
        match = re.search(rf"const {name}=.*?;", HTML)
        assert match is not None
        definitions.append(match.group(0))
    completed = run(
        [
            "node",
            "-e",
            "".join(definitions)
            + f"process.stdout.write(formatCpuTriple({used},{allocated},{allocatable}));",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


def evaluate_pod_phases(phases: dict[str, int] | None, evicted: int) -> str:
    from swegen.dashboard.server import HTML

    match = re.search(r"const formatPodPhases=.*?\};", HTML)
    assert match is not None
    completed = run(
        [
            "node",
            "-e",
            match.group(0)
            + f"process.stdout.write(formatPodPhases({json.dumps(phases)},{evicted}));",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


def evaluate_chart_axis_helpers(
    point_count: int,
    chart_width: int,
    count: int,
) -> tuple[int, str, str]:
    from swegen.dashboard.server import HTML

    definitions = []
    for name in ("compactChartCount", "compactChartTimestamp", "chartTickEvery"):
        match = re.search(rf"const {name}=.*?;", HTML)
        assert match is not None
        definitions.append(match.group(0))
    completed = run(
        [
            "node",
            "-e",
            "".join(definitions)
            + "process.stdout.write(JSON.stringify(["
            + f"chartTickEvery({point_count},{chart_width}),"
            + f"compactChartCount({count}),"
            + "compactChartTimestamp('2026-07-31T12:30:00Z')]));",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    import json

    tick_every, compact_count, timestamp = json.loads(completed.stdout)
    return int(tick_every), str(compact_count), str(timestamp)


def evaluate_chart_tooltip() -> dict[str, object]:
    from swegen.dashboard.server import HTML

    functions = (
        "function positionChartTooltip"
        + HTML.split("function positionChartTooltip", 1)[1].split("function scaleControls", 1)[0]
    )
    script = """
const tooltip={hidden:true,textContent:'',style:{},offsetWidth:100,offsetHeight:30};
const el=id=>tooltip;
function setText(node,value){node.textContent=value==null?'—':String(value)}
const window={innerWidth:500,innerHeight:300};
const event={type:'mouseenter',clientX:40,clientY:50,currentTarget:{getBoundingClientRect(){return {left:0,top:0,width:10}}}};
showChartTooltip(event,'Jul 31\\ntotal 3 - success|failed\\nmodel-a: 3|1');
const shown={hidden:tooltip.hidden,text:tooltip.textContent,left:tooltip.style.left,top:tooltip.style.top};
hideChartTooltip();
process.stdout.write(JSON.stringify({shown,hiddenAfterLeave:tooltip.hidden}));
"""
    completed = run(
        ["node", "-e", functions + script],
        check=True,
        capture_output=True,
        text=True,
    )
    import json

    return json.loads(completed.stdout)


def test_chart_first_render_defaults_to_the_rightmost_position() -> None:
    from swegen.dashboard.server import HTML

    assert evaluate_chart_scroll_target(None, 417) == 417
    assert "restoreChartScroll(chart,stage)" in HTML
    assert "requestAnimationFrame" in HTML


def test_chart_refresh_preserves_a_user_selected_scroll_position() -> None:
    from swegen.dashboard.server import HTML

    saved = {"left": 137, "followLatest": False}
    assert evaluate_chart_scroll_target(saved, 417) == 137
    assert evaluate_chart_scroll_target({"left": 0, "followLatest": False}, 417) == 0
    assert evaluate_chart_scroll_target(saved, 100) == 100
    assert "hasOwnProperty.call(uiState.chartScroll,stage)" in HTML
    # The surviving diverging chart keys its scroll state by the per-stage
    # scrollKey (`${stage}-model`), set on both the scroll listener and restore.
    assert "uiState.chartScroll[scrollKey]=chartScrollSnapshot(chart)" in HTML
    assert "restoreChartScroll(chart,scrollKey)" in HTML
    assert "chart.dataset.restoringScroll!=='true'" in HTML


def test_chart_refresh_keeps_follow_latest_pinned_to_new_right_edge() -> None:
    from swegen.dashboard.server import HTML

    at_right = evaluate_chart_scroll_snapshot(417, 617, 200)
    historical = evaluate_chart_scroll_snapshot(137, 617, 200)

    assert at_right == {"left": 417, "followLatest": True}
    assert historical == {"left": 137, "followLatest": False}
    assert evaluate_chart_scroll_target(at_right, 505) == 505
    assert evaluate_chart_scroll_target(historical, 505) == 137
    assert evaluate_chart_restore_sequence(at_right) == {
        "left": 505,
        "state": {"left": 505, "followLatest": True},
        "restoring": None,
    }
    assert evaluate_chart_restore_sequence(historical) == {
        "left": 137,
        "state": {"left": 137, "followLatest": False},
        "restoring": None,
    }
    assert "requestAnimationFrame(()=>requestAnimationFrame" in HTML
    assert "apply();requestAnimationFrame(()=>{apply()" in HTML


def test_chart_tooltip_appears_immediately_and_hides_on_leave() -> None:
    from swegen.dashboard.server import HTML

    result = evaluate_chart_tooltip()

    assert result == {
        "shown": {
            "hidden": False,
            # The tooltip renders as a compact multi-line block (newline-joined),
            # not a single wide middle-dot line.
            "text": "Jul 31\ntotal 3 - success|failed\nmodel-a: 3|1",
            "left": "52px",
            "top": "62px",
        },
        "hiddenAfterLeave": True,
    }
    assert "bucket.title=" not in HTML
    # The bucket element is named `cell` in the diverging renderer; the hover
    # handlers read the tooltip off the bucket record so an in-place update can
    # refresh the text without rebinding listeners.
    assert "cell.addEventListener('mouseenter'" in HTML
    assert "cell.addEventListener('mousemove',positionChartTooltip)" in HTML
    assert "showChartTooltip(event,record.tooltip,record.tooltipNodes)" in HTML
    # Newlines must render as line breaks: the tooltip CSS uses pre, not nowrap.
    assert "white-space:pre}.chart-tooltip[hidden]" in HTML
    assert "white-space:nowrap}.chart-tooltip" not in HTML
    assert 'id="chart-tooltip"' in HTML


def test_chart_axes_show_counts_and_compact_timestamp_ticks() -> None:
    from swegen.dashboard.server import HTML

    tick_every, compact_count, timestamp = evaluate_chart_axis_helpers(24, 240, 12_500)

    assert tick_every == 6
    assert compact_count == "12.5k"
    assert re.fullmatch(r"\d{2}:\d{2}", timestamp)
    assert "chart.className='chart chart-diverging'" in HTML
    assert "yAxis.className='chart-y-axis'" in HTML
    assert "xTick.className='x-tick'" in HTML
    assert "compactChartTimestamp(bucket.t)" in HTML
    assert ".x-tick::before{" in HTML


def test_chart_timestamp_density_responds_to_available_width() -> None:
    from swegen.dashboard.server import HTML

    narrow, _, _ = evaluate_chart_axis_helpers(24, 240, 950)
    wide, compact_count, _ = evaluate_chart_axis_helpers(24, 960, 1_250_000)

    assert narrow == 6
    assert wide == 2
    assert compact_count == "1.3m"
    assert "new ResizeObserver(()=>{updateChartTicks(chart);rescaleVisible(chart)})" in HTML
    assert "index!==0&&index!==ticks.length-1" in HTML


def test_cpu_triple_formats_used_allocated_and_allocatable_in_order() -> None:
    assert evaluate_cpu_triple("1250", "2500", "4000") == ("1.25 (31.3)% / 2.5 (62.5)% / 4 cores")


def test_cpu_triple_preserves_meaningful_fractional_cores() -> None:
    assert evaluate_cpu_triple("254", "1500", "192000") == ("0.254 (0.1)% / 1.5 (0.8)% / 192 cores")
    assert evaluate_cpu_triple("40747", "498200", "768000") == (
        "40.7 (5.3)% / 498.2 (64.9)% / 768 cores"
    )


def test_cpu_triple_handles_zero_or_missing_capacity_without_invalid_numbers() -> None:
    assert evaluate_cpu_triple("100", "undefined", "1000") == ("0.1 (10)% / — / 1 cores")
    unavailable = evaluate_cpu_triple("100", "200", "0")
    assert unavailable == "— / — / — cores"
    assert "NaN" not in unavailable
    assert "Infinity" not in unavailable


def evaluate_metrics_banner_helpers(expressions: list[str]) -> list[str]:
    from swegen.dashboard.server import HTML

    definitions = []
    for name in ("agoUnit", "relativeAge", "friendlyError"):
        match = re.search(rf"const {name}=.*?;\n", HTML)
        assert match is not None
        definitions.append(match.group(0))
    completed = run(
        [
            "node",
            "-e",
            "".join(definitions)
            + "const now=Date.now();const ago=s=>new Date(now-s*1000).toISOString();"
            + f"process.stdout.write(JSON.stringify([{','.join(expressions)}]));",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def test_stale_metrics_age_reads_in_human_units_not_iso_timestamps() -> None:
    seconds, minutes, hours, days = evaluate_metrics_banner_helpers(
        [
            "relativeAge(ago(5))",
            "relativeAge(ago(240))",
            "relativeAge(ago(7200))",
            "relativeAge(ago(172800))",
        ]
    )
    assert seconds == "5 seconds ago"
    assert minutes == "4 minutes ago"
    assert hours == "2 hours ago"
    assert days == "2 days ago"


def test_relative_age_singularises_and_survives_unusable_timestamps() -> None:
    one_second, one_minute, missing, garbage = evaluate_metrics_banner_helpers(
        [
            "relativeAge(ago(1))",
            "relativeAge(ago(60))",
            "relativeAge(null)",
            "relativeAge('not-a-timestamp')",
        ]
    )
    assert one_second == "1 second ago"
    assert one_minute == "1 minute ago"
    # A timestamp we cannot parse must not leak back into the banner verbatim.
    assert missing == garbage == "an unknown time ago"


def test_banner_errors_drop_python_exception_and_kubectl_prefixes() -> None:
    runtime, kubectl, timeout, plain, empty = evaluate_metrics_banner_helpers(
        [
            "friendlyError('RuntimeError: error: Metrics API not available')",
            "friendlyError('error: Metrics API not available')",
            "friendlyError('TimeoutError: deadline exceeded')",
            "friendlyError('metrics-server is starting')",
            "friendlyError(null)",
        ]
    )
    assert runtime == kubectl == "Metrics API not available"
    assert timeout == "deadline exceeded"
    assert plain == "metrics-server is starting"
    assert empty == ""


def test_resource_banner_reports_staleness_without_raw_python_errors() -> None:
    from swegen.dashboard.server import HTML

    body = HTML.split("function renderResourcesV2", 1)[1]
    banner = next(line for line in body.splitlines() if "Resource metrics unavailable" in line)

    assert "relativeAge(metrics.collected_at)" in banner
    assert "friendlyError(metrics.error)" in banner
    # The old banner interpolated the ISO timestamp and the bare exception text
    # directly; only guarded reads of those fields may remain.
    assert "${metrics.collected_at||'unknown time'}" not in banner
    assert "${metrics.error||'refresh failed'}" not in banner
    assert "${metrics.error}" not in banner


def test_dashboard_places_resources_and_storage_immediately_before_recent_tasks() -> None:
    from swegen.dashboard.server import HTML

    stages = HTML.index("<h2>Stages</h2>")
    resources = HTML.index("<h2>Cluster resources</h2>")
    storage = HTML.index("<h2>Harbor task storage</h2>")
    buildkit_farm = HTML.index("<h2>Remote BuildKit farm</h2>")
    recent_tasks = HTML.index("<h2>Recent tasks</h2>")

    assert stages < resources < storage < buildkit_farm < recent_tasks
    # Hourly yield is no longer a standalone global panel; it now lives inside
    # each stage card beside that stage's timeseries chart.
    assert "<h2>Hourly yield</h2>" not in HTML
    assert 'id="yield"' not in HTML
    assert "renderHourlyYield" not in HTML
    assert "<h2>15-minute outcomes</h2>" not in HTML
    assert 'id="timeline"' not in HTML
    assert "<h2>Task states</h2>" not in HTML
    assert 'id="states"' not in HTML
    assert "el('states')" not in HTML


def test_remote_buildkit_farm_renders_safe_read_only_status_cards() -> None:
    from swegen.dashboard.server import HTML

    assert 'id="buildkit-farm-status"' in HTML
    assert 'id="buildkit-farm-summary"' in HTML
    assert "renderRemoteBuildKit(data.buildkit_farm||{},pg.remote_builds||{})" in HTML
    for title in (
        "Gateway / ready",
        "Backend workers",
        "Sampled live queue",
        "Sampled live active",
        "SWEgen submission ledger",
    ):
        assert title in HTML
    assert "stale excluded" in HTML
    assert "database submission ledger only, not live farm state" in HTML
    assert "resources.last_success_at" in HTML
    assert "SWEgen remote pending" not in HTML
    assert "registry_password" not in HTML
    assert "registry_username" not in HTML


def test_local_and_remote_node_disk_io_views_are_compact_and_graceful() -> None:
    from swegen.dashboard.server import HTML

    assert "Disk I/O read / write · IOPS · busy" in HTML
    # The node cell goes through diskIoLabel(), which wraps formatDiskIo() with
    # the sample's age and its per-node breaker state. The bare formatter would
    # render a frozen sample exactly like a live one.
    assert "diskIoLabel(node.disk_io)" in HTML
    assert "formatDiskIo(io)" in HTML
    assert "Live cAdvisor rate sample over" in HTML
    assert 'id="buildkit-farm-disk-io"' in HTML
    assert "resources.node_disk_io||[]" in HTML
    assert "Remote per-node disk I/O telemetry is not exposed" in HTML
    assert "rateBytes(row.read_bytes_per_second)" in HTML


def test_local_buildkit_slots_are_shown_per_node_with_unknown_waiters() -> None:
    from swegen.dashboard.server import HTML

    assert "Local BuildKit slots / waiters" in HTML
    assert "buildSlotControls(workload,node.build_slots)" in HTML
    assert "setText(status,formatBuildSlots(slots))" in HTML
    assert "${slots.used}/${slots.total} used" in HTML
    assert "waiters ${slots.waiters??'unknown'}" in HTML
    assert "Wrapper does not persist waiter depth; unknown is explicit." in HTML
    assert "td.colSpan=5" in HTML


def test_local_buildkit_slots_have_compact_per_node_apply_controls() -> None:
    from swegen.dashboard.server import HTML

    assert 'id="build-slot-feedback"' in HTML
    assert "function buildSlotControls(node,slots)" in HTML
    assert "input.type='number';input.min='1'" in HTML
    assert "setText(button,'Apply')" in HTML
    assert ".slot-controls{display:grid;grid-template-columns:72px 52px" in HTML
    assert "fetch('/api/pipeline/build-slots'" in HTML
    assert "JSON.stringify({node,slots})" in HTML
    assert "in-flight builds on retired slots finish normally" in HTML


def test_top_stage_cards_use_display_names_and_omit_dead_letters() -> None:
    from swegen.dashboard.server import HTML

    assert "<b>${stageNames[stage]}</b>" in HTML
    assert "stage.toUpperCase()" not in HTML
    assert "DEAD LETTERS" not in HTML
    for title in ("SWEgen", "NOP / Oracle", "Repair", "Reward hack", "SWR push"):
        assert title in HTML


def test_top_stage_cards_display_lifetime_processed_count() -> None:
    from swegen.dashboard.server import HTML

    assert "pg.throughput?.lifetime_processed?.[stage]" in HTML
    # The stat text is built by stageStatLines so a poll can rewrite it in place.
    assert "lifetime:`lifetime processed ${lifetime}`" in HTML


def evaluate_instance_coverage(count: object, total: object) -> str:
    from swegen.dashboard.server import HTML

    match = re.search(r"function formatInstanceCoverage\(count,total\)\{.*?\}\n", HTML)
    assert match is not None
    completed = run(
        [
            "node",
            "-e",
            match.group(0)
            + f"process.stdout.write(formatInstanceCoverage({count},{total}));",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


def test_top_stage_cards_render_unique_instance_coverage_fraction() -> None:
    from swegen.dashboard.server import HTML

    # Every stage card pulls its numerator/denominator from the shared coverage
    # section and renders a "<count> / <total> (<pct>%)" fraction.
    assert "pg.instance_coverage||{}" in HTML
    assert "cov.unique_instances_processed?.[stage]||0" in HTML
    assert "unique:`unique iids ${formatInstanceCoverage(unique,universe)}`" in HTML


def test_instance_coverage_fraction_formats_count_total_and_percent() -> None:
    assert evaluate_instance_coverage(68334, 208659) == "68334 / 208659 (32.75%)"
    # A missing/failed denominator degrades to an em dash, not a division by zero.
    assert evaluate_instance_coverage(6217, "null") == "6217 / —"
    assert evaluate_instance_coverage(6217, 0) == "6217 / —"
    assert evaluate_instance_coverage("null", 208659) == "0 / 208659 (0.00%)"


# A minimal DOM/globals stub so DOM-building chart helpers run under bare node.
_DOM_STUB = r"""
class El{constructor(tag){this.tag=tag;this.children=[];this.className='';this.style={};this.dataset={};this.attrs={};this.textContent='';}
  append(...cs){for(const c of cs)this.children.push(c);}
  setAttribute(k,v){this.attrs[k]=v;} addEventListener(){}
  querySelectorAll(){return [];}
  get clientWidth(){return 400;} get scrollWidth(){return 400;} get clientHeight(){return 112;}}
globalThis.document={createElement:t=>new El(t),getElementById:()=>new El('div')};
globalThis.window={innerWidth:1200,innerHeight:800};
globalThis.requestAnimationFrame=fn=>fn();
globalThis.ResizeObserver=undefined;
globalThis.uiState={chartScroll:{}};
globalThis.setText=(node,v)=>{node.textContent=String(v);};
globalThis.el=()=>new El('div');
globalThis.showChartTooltip=()=>{};globalThis.positionChartTooltip=()=>{};globalThis.hideChartTooltip=()=>{};
globalThis.updateChartTicks=()=>{};globalThis.chartScrollSnapshot=()=>({left:0,followLatest:true});
globalThis.restoreChartScroll=()=>{};
"""


def _model_palette() -> list[str]:
    """The MODEL_PALETTE hex list as declared in the dashboard HTML."""

    from swegen.dashboard.server import HTML

    match = re.search(r"const MODEL_PALETTE=\[(.*?)\];", HTML)
    assert match is not None
    return re.findall(r"#[0-9a-fA-F]{6}", match.group(1))


def render_diverging_model_chart(stage_data: object, stage: str = "generate") -> dict[str, object]:
    from swegen.dashboard.server import HTML

    consts = "".join(
        re.search(re.escape(prefix) + r".*?;\n", HTML).group(0)
        for prefix in (
            "const MODEL_PALETTE=",
            "const modelColorIndex=",
            "const modelColor=",
            "const compactChartCount=",
            "const compactChartTimestamp=",
        )
    )
    renderer = re.search(
        r"function rescaleVisible\(chart\)\{.*?\n(?=function validateQueueLine)",
        HTML,
        re.S,
    )
    assert renderer is not None
    program = (
        _DOM_STUB
        + consts
        + renderer.group(0)
        + f"const wrap=divergingModelTimeSeries({json.dumps(stage_data)},{json.dumps(stage)},48);"
        + r"""
let up=[],down=[],legend=[],rejected=0,scrollKey=null,tooltips=[];
(function walk(node,slot){const cls=node.className||'';
  if(cls.includes('chart-diverging'))scrollKey=node.dataset.stage;
  if(cls.includes('bar-slot-up'))slot='up';
  if(cls.includes('bar-slot-down'))slot='down';
  if(cls==='bucket'&&node.attrs&&node.attrs['aria-label']!=null)tooltips.push(node.attrs['aria-label']);
  if(cls==='segment')(slot==='up'?up:down).push({bg:node.style.background,op:node.style.opacity||'1'});
  if(cls==='rejected-marker')rejected++;
  if(cls==='diverging-legend-swatch')legend.push(node.style.background);
  (node.children||[]).forEach(c=>walk(c,slot));})(wrap,null);
const text=JSON.stringify(wrap).includes('chart-empty');
process.stdout.write(JSON.stringify({up,down,legend,rejected,scrollKey,tooltips,empty:text}));
"""
    )
    completed = run(
        ["node", "-e", program], check=True, capture_output=True, text=True
    )
    return json.loads(completed.stdout)


def capture_diverging_tooltip_nodes(stage_data: object) -> dict[str, object]:
    """Fire a bucket's mouseenter handler and inspect the tooltip nodes it shows.

    The rich tooltip nodes live only in the event-handler closure (not the
    rendered tree), so a plain tree-walk can't see them. Here addEventListener
    records handlers, showChartTooltip captures the nodes argument, and we invoke
    the first bucket's mouseenter to read back the per-model rows + swatches.
    """

    from swegen.dashboard.server import HTML

    consts = "".join(
        re.search(re.escape(prefix) + r".*?;\n", HTML).group(0)
        for prefix in (
            "const MODEL_PALETTE=",
            "const modelColorIndex=",
            "const modelColor=",
            "const compactChartCount=",
            "const compactChartTimestamp=",
        )
    )
    renderer = re.search(
        r"function rescaleVisible\(chart\)\{.*?\n(?=function validateQueueLine)",
        HTML,
        re.S,
    )
    assert renderer is not None
    # A DOM stub whose addEventListener records handlers, and a showChartTooltip
    # that captures the nodes it is handed.
    stub = r"""
class El{constructor(tag){this.tag=tag;this.children=[];this.className='';this.style={};this.dataset={};this.attrs={};this.textContent='';this.handlers={};}
  append(...cs){for(const c of cs)this.children.push(c);}
  replaceChildren(...cs){this.children=cs;}
  setAttribute(k,v){this.attrs[k]=v;} addEventListener(t,fn){this.handlers[t]=fn;}
  querySelectorAll(){return [];}
  get clientWidth(){return 400;} get scrollWidth(){return 400;} get clientHeight(){return 112;}}
globalThis.document={createElement:t=>new El(t),getElementById:()=>new El('div')};
globalThis.window={innerWidth:1200,innerHeight:800};
globalThis.requestAnimationFrame=fn=>fn();globalThis.ResizeObserver=undefined;
globalThis.uiState={chartScroll:{}};
globalThis.setText=(node,v)=>{node.textContent=String(v);};
globalThis.el=()=>new El('div');
let CAPTURED=null;
globalThis.showChartTooltip=(event,value,nodes)=>{CAPTURED={value,nodes};};
globalThis.positionChartTooltip=()=>{};globalThis.hideChartTooltip=()=>{};
globalThis.updateChartTicks=()=>{};globalThis.chartScrollSnapshot=()=>({left:0,followLatest:true});
globalThis.restoreChartScroll=()=>{};
"""
    program = (
        stub
        + consts
        + renderer.group(0)
        + f"const wrap=divergingModelTimeSeries({json.dumps(stage_data)},'generate',48);"
        + r"""
let firstBucket=null;
(function walk(node){if((node.className||'')==='bucket'&&!firstBucket)firstBucket=node;(node.children||[]).forEach(walk);})(wrap);
firstBucket.handlers.mouseenter({});
const rows=(CAPTURED.nodes||[]).filter(n=>(n.className||'')==='chart-tooltip-row').map(row=>{
  const sw=row.children.find(c=>(c.className||'')==='chart-tooltip-swatch');
  const txt=row.children.find(c=>(c.className||'')!=='chart-tooltip-swatch');
  return {swatch:sw?sw.style.background:null,text:txt?txt.textContent:null};});
process.stdout.write(JSON.stringify({ariaLabel:CAPTURED.value,rows}));
"""
    )
    completed = run(
        ["node", "-e", program], check=True, capture_output=True, text=True
    )
    return json.loads(completed.stdout)


def test_diverging_tooltip_rows_carry_the_per_model_colour_swatch() -> None:
    from swegen.dashboard.server import HTML

    # CSS for the swatch + row exists.
    assert ".chart-tooltip-swatch{" in HTML
    assert ".chart-tooltip-row{" in HTML
    # showChartTooltip renders DOM nodes when given them, else falls back to text.
    assert "function showChartTooltip(event,value,nodes)" in HTML
    assert "if(nodes){tooltip.replaceChildren(...nodes)}else{setText(tooltip,value)}" in HTML

    stage_data = {
        "models": ["deepseek-v4-flash", "glm-5.2-moedsa"],
        "buckets": [
            {
                "t": "2026-08-07T10:00:00+00:00",
                "by_model": {
                    "deepseek-v4-flash": {"succeeded": 5, "failed": 1, "rejected": 0},
                    "glm-5.2-moedsa": {"succeeded": 2, "failed": 3, "rejected": 0},
                },
            }
        ],
    }
    result = capture_diverging_tooltip_nodes(stage_data)

    # One row per active model, each carrying a non-empty colour swatch that
    # matches the model's legend colour, and the model's outcome text.
    rows = result["rows"]
    assert len(rows) == 2
    swatches = [row["swatch"] for row in rows]
    assert all(swatches), "every tooltip row must have a colour swatch"
    # Distinct colours per model (they take distinct palette slots).
    assert swatches[0] != swatches[1]
    assert rows[0]["text"].startswith("deepseek-v4-flash: 5|1")
    assert rows[1]["text"].startswith("glm-5.2-moedsa: 2|3")
    # The plain-text aria-label is retained for screen readers.
    assert "deepseek-v4-flash: 5|1" in result["ariaLabel"]
    assert "\n" in result["ariaLabel"]


def test_all_stages_route_to_the_diverging_per_model_chart() -> None:
    from swegen.dashboard.server import HTML

    # Every stage card now renders the shared per-model diverging chart, reading
    # its stage's slice of the unified stage_model_timeseries structure.
    assert (
        "divergingModelTimeSeries(pg.stage_model_timeseries?.stages?.[stage],stage,"
        "pg.stage_model_timeseries?.lookback_hours)"
        in HTML
    )
    assert "function divergingModelTimeSeries(stageData,stage,rangeHours)" in HTML
    # The generate-only special-case ternary is gone.
    assert "stage==='generate'?generateModelTimeSeries" not in HTML
    assert "function generateModelTimeSeries" not in HTML
    # A central baseline (bottom border on the up slot) and diverging slots exist.
    assert ".bar-slot-up{align-items:flex-end;border-bottom:1px solid var(--line)}" in HTML
    assert ".bar-slot-down{align-items:flex-start}" in HTML
    # The y-axis diverges around zero, scaling the top half to success and the
    # bottom half to failure INDEPENDENTLY (upMax, 0, -downMax).
    assert "[upMax,0,-downMax].forEach" in HTML
    assert "[max,0,-max].forEach" not in HTML
    assert "success up · failed down" in HTML
    # Per-stage scroll keys are distinct (stage-derived, not a hardcoded literal).
    assert "const scrollKey=`${stage}-model`" in HTML
    assert "chart.dataset.stage=scrollKey" in HTML
    assert "restoreChartScroll(chart,scrollKey)" in HTML


def evaluate_rescale_visible(
    buckets: list[dict[str, int]],
    scroll_left: int,
    client_width: int,
    chart_offset_left: int = 0,
) -> dict[str, object]:
    """Drive rescaleVisible over stub bucket cells with explicit geometry.

    Each entry in ``buckets`` is ``{up, down, left, width}``; the stub places the
    cell's offset box at ``left``/``width`` so the visible-window computation can
    be exercised without a real layout engine. Returns the resulting bar heights
    and the y-axis tick labels.

    ``chart_offset_left`` models the chart's own position within its
    offsetParent. Real ``offsetLeft`` values are measured from the nearest
    POSITIONED ancestor, so when the chart sits partway across the page every
    cell's offsetLeft carries that page offset while ``scrollLeft`` does not.
    Passing a non-zero value here (with cell ``left`` values shifted to match)
    reproduces that mixed-coordinate case.
    """

    from swegen.dashboard.server import HTML

    rescale = (
        "function rescaleVisible"
        + HTML.split("function rescaleVisible", 1)[1].split("function scheduleRescaleVisible", 1)[0]
    )
    # rescaleVisible delegates every height write to applyBucketHeights; pull that
    # shared writer in too so the harness exercises the real mapping.
    apply_heights = "function applyBucketHeights" + HTML.split(
        "function applyBucketHeights", 1
    )[1].split("\n", 1)[0]
    compact = re.search(r"const compactChartCount=.*?;", HTML)
    assert compact is not None
    program = (
        compact.group(0)
        + "function setText(node,value){node.textContent=value==null?'—':String(value)}\n"
        # Minimal classList stub so the 'clipped' marking is observable.
        + """
function stubBar(){const set=new Set();return {style:{},classList:{
  toggle:(name,on)=>{if(on)set.add(name);else set.delete(name)},
  has:name=>set.has(name)}}}
"""
        + apply_heights
        + "\n"
        + rescale
        + f"""
const data={json.dumps(buckets)};
const cellData=data.map(b=>({{
  cell:{{offsetLeft:b.left,offsetWidth:b.width}},
  up:b.up,down:b.down,
  upBar:stubBar(),downBar:stubBar(),
  upSegs:[{{seg:{{style:{{}}}},value:b.up}}],
  downSegs:[{{seg:{{style:{{}}}},value:b.down}}],
}}));
const yTicks=[{{textContent:''}},{{textContent:''}},{{textContent:''}}];
const chart={{_buckets:cellData,_yAxis:{{children:yTicks}},scrollLeft:{scroll_left},clientWidth:{client_width},offsetLeft:{chart_offset_left}}};
rescaleVisible(chart);
process.stdout.write(JSON.stringify({{
  upHeights:cellData.map(c=>c.upBar.style.height),
  downHeights:cellData.map(c=>c.downBar.style.height),
  upClipped:cellData.map(c=>c.upBar.classList.has('clipped')),
  downClipped:cellData.map(c=>c.downBar.classList.has('clipped')),
  ticks:yTicks.map(t=>t.textContent),
}}));
"""
    )
    completed = run(["node", "-e", program], check=True, capture_output=True, text=True)
    return json.loads(completed.stdout)


def test_rescale_visible_scales_to_only_the_visible_bucket_subset() -> None:
    from swegen.dashboard.server import HTML

    # A giant spike sits at the far right (offset 900); the visible window covers
    # only the first three buckets (offsets 0..30, each width 10). The vertical
    # scale must come from the visible max (up=20), NOT the off-screen spike (500).
    buckets = [
        {"up": 10, "down": 4, "left": 0, "width": 10},
        {"up": 20, "down": 8, "left": 10, "width": 10},
        {"up": 5, "down": 2, "left": 20, "width": 10},
        {"up": 500, "down": 300, "left": 900, "width": 10},
    ]
    result = evaluate_rescale_visible(buckets, scroll_left=0, client_width=30)

    # The up and down halves scale INDEPENDENTLY over the visible window:
    # visibleUpMax = max(up) = 20, visibleDownMax = max(down) = 8, so the axis
    # ticks read [20, 0, -8] (different magnitudes).
    assert result["ticks"] == ["20", "0", "-8"]
    assert result["upHeights"][1] == "100%"
    assert result["upHeights"][0] == "50%"
    # The off-screen spike never set the scale (else the visible bars would be a
    # few percent tall). It is also NOT written at all while off-screen: applying
    # the visible denominator to it would clamp 500 to 100% and render it
    # identically to the on-screen 20 — the clipping bug. It keeps whatever the
    # last in-scope pass gave it and is recomputed when it scrolls in.
    assert result["upHeights"][3] is None
    assert result["upClipped"][3] is False

    # Scroll so only the spike is visible: now each half's scale jumps to it
    # (up=500, down=300) independently.
    scrolled = evaluate_rescale_visible(buckets, scroll_left=895, client_width=30)
    assert scrolled["ticks"] == ["500", "0", "-300"]
    # The spike is now in scope and renders at full height against its own max --
    # crucially at its TRUE height, not clamped, and therefore not marked clipped.
    assert scrolled["upHeights"][3] == "100%"
    assert scrolled["upClipped"][3] is False
    # The small early buckets are now off-screen and are left untouched rather
    # than being rewritten against a denominator they are not being measured by.
    assert scrolled["upHeights"][0] is None

    # The wiring: rescaleVisible runs on scroll (throttled) and on resize.
    assert "scheduleRescaleVisible(chart)" in HTML
    assert "chart._rescalePending" in HTML
    assert "new ResizeObserver(()=>{updateChartTicks(chart);rescaleVisible(chart)})" in HTML
    assert "requestAnimationFrame(()=>{updateChartTicks(chart);rescaleVisible(chart)})" in HTML
    # Per-bucket totals + segments are stored on the chart so rescale never
    # re-reads DOM text.
    assert "chart._buckets=cellData" in HTML
    assert "cellData.push(record)" in HTML
    assert "const record={cell,up,down,upBar:null,downBar:null,upSegs:[],downSegs:[]" in HTML


def test_tall_buckets_are_never_silently_clipped_to_the_visible_window_max() -> None:
    # REGRESSION: the operator saw generate bars "clip above ~213". 213 was not a
    # constant anywhere -- it was simply the tallest bucket in their viewport.
    # rescaleVisible computed the denominator from the VISIBLE cells but wrote
    # heights to EVERY cell, so all 9 buckets taller than 213 were clamped by
    # Math.min(100,...) and rendered flat-topped and indistinguishable, while the
    # y-axis claimed the top was 213. These are the real leading values from
    # /api/pipeline/status for the generate stage.
    up_series = [111, 168, 0, 0, 12, 213, 95, 157, 135, 195, 143, 0, 0, 0, 0, 0]
    tall = [251, 306, 347, 380, 456, 513, 572, 529, 450]
    values = up_series + tall
    buckets = [
        {"up": v, "down": 0, "left": i * 10, "width": 10} for i, v in enumerate(values)
    ]
    # Viewport shows only the first 8 buckets, whose max is exactly 213.
    result = evaluate_rescale_visible(buckets, scroll_left=0, client_width=80)

    assert result["ticks"][0] == "213"
    # In-scope bars scale honestly against the axis they are drawn next to.
    assert result["upHeights"][5] == "100%"  # 213 == the max, full height
    assert result["upHeights"][1] == f"{168 / 213 * 100}%"
    # No in-scope bar is clipped, because the denominator IS the in-scope max.
    assert not any(result["upClipped"][:8])
    # The 572 bucket (and every other tall one) is off-screen and therefore not
    # written against a 213 denominator. Under the old code each of these read
    # "100%" -- identical to the 213 bar, a 2.7x misrepresentation.
    for index in range(len(up_series), len(values)):
        assert result["upHeights"][index] is None, index
        assert result["upClipped"][index] is False, index

    # Scrolling to the tall region rescales the axis to the real peak, and those
    # bars then render at their true relative heights instead of a flat cap.
    scrolled = evaluate_rescale_visible(buckets, scroll_left=160, client_width=90)
    assert scrolled["ticks"][0] == "572"
    peak = values.index(572)
    assert scrolled["upHeights"][peak] == "100%"
    assert scrolled["upClipped"][peak] is False
    # 251 against a 572 max is ~44% -- visibly shorter than the peak, where the
    # buggy version drew both at 100%.
    assert scrolled["upHeights"][values.index(251)] == f"{251 / 572 * 100}%"


def test_visible_window_is_measured_in_the_charts_own_coordinate_frame() -> None:
    # REGRESSION: a cell's offsetLeft is measured from its nearest POSITIONED
    # ancestor. Nothing between .bucket and <body> was positioned, so offsetLeft
    # was a PAGE coordinate carrying the stage card's ~600px page offset, while
    # chart.scrollLeft is a CONTENT coordinate starting at 0. Comparing the two
    # shifted the "visible" window by that offset: on first paint (scrolled to
    # the newest data) the scope resolved to the OLDEST buckets, so the chart
    # scaled to their max and a 213 bucket rendered exactly as tall as a 572 one.
    from swegen.dashboard.server import HTML

    values = [111, 168, 12, 213, 95, 157, 251, 306, 456, 572]
    card_x, width, gap = 620, 12, 2
    # Cell offsets include the card's page offset, exactly as the browser reports.
    buckets = [
        {"up": v, "down": 0, "left": card_x + i * (width + gap), "width": width}
        for i, v in enumerate(values)
    ]
    client_width = 60  # shows ~4 buckets
    content_width = len(values) * (width + gap)
    scrolled_to_newest = max(0, content_width - client_width)

    result = evaluate_rescale_visible(
        buckets,
        scroll_left=scrolled_to_newest,
        client_width=client_width,
        chart_offset_left=card_x,
    )

    # The axis must report the peak of the NEWEST buckets (the scrolled-to
    # region), not the max of the stale leading ones.
    assert result["ticks"][0] == "572"
    peak = values.index(572)
    assert result["upHeights"][peak] == "100%"
    # And the mid-sized bar must be visibly shorter than the peak -- the exact
    # thing the operator reported as "200 looks as tall as 500".
    mid = values.index(251)
    assert result["upHeights"][mid] == f"{251 / 572 * 100}%"
    assert result["upHeights"][mid] != "100%"
    # The stale 213 bucket is off-screen and must not have set the scale.
    assert result["upHeights"][values.index(213)] is None

    # The chart establishes its own positioning context so offsetLeft and
    # scrollLeft share a frame by construction, and the subtraction keeps it
    # correct even if that CSS is removed.
    assert ".chart{position:relative;" in HTML
    assert "const originLeft=chart.offsetLeft||0" in HTML
    assert "const left=(c.cell.offsetLeft||0)-originLeft" in HTML


def test_clipped_bars_are_marked_so_truncation_is_never_silent() -> None:
    # The non-negotiable invariant: if a bar IS clamped, it must say so. We force
    # the condition by making a zero-width (always-"visible") cell set carry a
    # value above the denominator the axis reports.
    from swegen.dashboard.server import HTML

    # Both bars exceed the max of the *other* visible cell only if the scope is
    # wrong; with correct scoping the scope max covers every drawn bar, so the
    # marker stays off. This asserts the healthy case first.
    buckets = [
        {"up": 10, "down": 5, "left": 0, "width": 10},
        {"up": 500, "down": 300, "left": 10, "width": 10},
    ]
    healthy = evaluate_rescale_visible(buckets, scroll_left=0, client_width=20)
    assert healthy["ticks"] == ["500", "0", "-300"]
    assert healthy["upHeights"][1] == "100%"
    assert not any(healthy["upClipped"])
    assert not any(healthy["downClipped"])

    # The shared writer clamps AND marks: applyBucketHeights is the single place
    # a value becomes a height, used by both first paint and every rescale, so
    # the two can never disagree about the mapping.
    assert "function applyBucketHeights(c,upMax,downMax)" in HTML
    assert "classList.toggle('clipped',upPercent>100.0001)" in HTML
    assert "classList.toggle('clipped',downPercent>100.0001)" in HTML
    # Initial render routes through the same writer rather than hand-rolling the
    # height maths a second time.
    assert "applyBucketHeights(record,upMax,downMax)" in HTML
    # Heights are applied to the SCOPE, not to every cell -- this is the fix.
    assert "scope.forEach(c=>{applyBucketHeights(c,visibleUpMax,visibleDownMax)})" in HTML
    assert "cells.forEach(c=>{c.upBar.style.height=" not in HTML
    # And the marker has a visible affordance rather than being CSS-less state.
    assert ".bar.clipped::after" in HTML


def test_rescale_visible_scales_the_up_and_down_halves_independently() -> None:
    # A tall success spike in one bucket must NOT squash the failure bars in
    # another, and vice-versa: the top half scales to the success max, the bottom
    # half to the failure max, computed independently over the visible window.
    buckets = [
        # Bucket 0: big success, small failure.
        {"up": 100, "down": 10, "left": 0, "width": 10},
        # Bucket 1: small success, big failure. So visibleUpMax=100 (bucket 0),
        # visibleDownMax=80 (bucket 1) — the two maxima live in different cells.
        {"up": 5, "down": 80, "left": 10, "width": 10},
    ]
    result = evaluate_rescale_visible(buckets, scroll_left=0, client_width=30)

    # (a) The y-axis top tick reads the up-max (100) and the bottom tick reads
    # the DOWN-max (-80): different magnitudes, proving the halves are separate.
    assert result["ticks"] == ["100", "0", "-80"]

    # (b) The down bar of value 80 renders at full height because it equals
    # visibleDownMax — even though 80 < the up-max of 100. If a single combined
    # max were used it would only reach 80% and this assertion would fail.
    assert result["downHeights"][1] == "100%"
    # The up spike likewise reaches full height against its own max.
    assert result["upHeights"][0] == "100%"
    # Cross-check the smaller values scale against the correct per-half max:
    # down of 10 against downMax 80 -> 12.5%; up of 5 against upMax 100 -> 5%.
    assert result["downHeights"][0] == "12.5%"
    assert result["upHeights"][1] == "5%"

    # Scrolling to a subset recomputes BOTH maxima from only the visible cells:
    # window covers only bucket 1 (up=5, down=80).
    scrolled = evaluate_rescale_visible(buckets, scroll_left=10, client_width=10)
    assert scrolled["ticks"] == ["5", "0", "-80"]
    assert scrolled["upHeights"][1] == "100%"
    assert scrolled["downHeights"][1] == "100%"


def _diverging_legend_colors_program(renders: list[dict[str, object]]) -> str:
    """Build a node program that renders divergingModelTimeSeries N times in ONE
    process (sharing uiState) and returns each render's model -> legend colour.

    ``renders`` is a list of ``{"stage": str, "models": [...]}``. Each render's
    stage_data carries one bucket giving every listed model a success + failure
    so it appears in both the legend and the bars. The returned JSON is a list
    (one per render) of ``{model_id: legendColor}`` maps, letting a test assert a
    model's colour is invariant as new models appear and across stage cards.
    """

    from swegen.dashboard.server import HTML

    consts = "".join(
        re.search(re.escape(prefix) + r".*?;\n", HTML).group(0)
        for prefix in (
            "const MODEL_PALETTE=",
            "const modelColorIndex=",
            "const modelColor=",
            "const compactChartCount=",
            "const compactChartTimestamp=",
        )
    )
    renderer = re.search(
        r"function rescaleVisible\(chart\)\{.*?\n(?=function validateQueueLine)",
        HTML,
        re.S,
    )
    assert renderer is not None
    payload = []
    for spec in renders:
        stage_data = {
            "models": list(spec["models"]),
            "buckets": [
                {
                    "t": "2026-08-07T10:00:00Z",
                    "by_model": {
                        m: {"succeeded": 3, "failed": 1, "rejected": 0}
                        for m in spec["models"]
                    },
                }
            ],
        }
        payload.append({"stage": spec["stage"], "stageData": stage_data})
    return (
        _DOM_STUB
        + consts
        + renderer.group(0)
        + f"const RENDERS={json.dumps(payload)};"
        + r"""
const results=RENDERS.map(r=>{
  const wrap=divergingModelTimeSeries(r.stageData,r.stage,48);
  const colors={};const models=r.stageData.models;let mi=0;
  (function walk(node){const cls=node.className||'';
    if(cls==='diverging-legend-swatch'){colors[models[mi++]]=node.style.background}
    (node.children||[]).forEach(walk)})(wrap);
  return colors;
});
process.stdout.write(JSON.stringify(results));
"""
    )


def test_model_colours_are_stable_when_new_models_appear_and_across_stages() -> None:
    # The colour of a model_id must be a stable function of the id string, not of
    # its position in the (sorted) models array. Render once, then again with a
    # NEW model inserted in the middle of the sorted order; the pre-existing
    # models must keep their exact colours (this fails under the old index-based
    # scheme, which shifts everything that sorts after the new id).
    program = _diverging_legend_colors_program(
        [
            {"stage": "generate", "models": ["glm-5.2-moedsa", "deepseek-v4-flash"]},
            # 'a-new-model' sorts between 'deepseek-v4-flash' and 'glm-5.2-moedsa',
            # so under the old index colouring glm's index (and colour) would move.
            {
                "stage": "generate",
                "models": ["a-new-model", "deepseek-v4-flash", "glm-5.2-moedsa"],
            },
            # A different stage card rendering the same ids must reuse the same
            # colours (per-model, not per-card).
            {"stage": "validate", "models": ["glm-5.2-moedsa", "deepseek-v4-flash"]},
        ]
    )
    completed = run(["node", "-e", program], check=True, capture_output=True, text=True)
    first, second, other_stage = json.loads(completed.stdout)

    # deepseek and glm keep their colours when 'a-new-model' appears.
    assert second["deepseek-v4-flash"] == first["deepseek-v4-flash"]
    assert second["glm-5.2-moedsa"] == first["glm-5.2-moedsa"]
    # The new model gets its own distinct colour (palette not exhausted at 3<8).
    assert second["a-new-model"] not in {
        first["deepseek-v4-flash"],
        first["glm-5.2-moedsa"],
    }
    # Same model_id -> same colour regardless of which stage card renders it.
    assert other_stage["glm-5.2-moedsa"] == first["glm-5.2-moedsa"]
    assert other_stage["deepseek-v4-flash"] == first["deepseek-v4-flash"]


def test_diverging_chart_works_for_a_non_generate_stage_with_distinct_scroll_key() -> None:
    # A downstream stage (validate) renders up/down segments coloured per model,
    # a per-model legend, and a scroll key namespaced to its own stage so its
    # scroll state never collides with the generate chart's.
    stage_data = {
        "models": ["glm-5.2-pretrain-v1", "deepseek-v4-flash"],
        "buckets": [
            {
                "t": "2026-08-07T10:00:00Z",
                "by_model": {
                    "glm-5.2-pretrain-v1": {"succeeded": 10, "failed": 2, "rejected": 0},
                    "deepseek-v4-flash": {"succeeded": 4, "failed": 6, "rejected": 0},
                },
            }
        ],
    }
    palette = _model_palette()
    rendered = render_diverging_model_chart(stage_data, "validate")
    assert rendered["empty"] is False
    up_colors = [seg["bg"] for seg in rendered["up"]]
    assert all(c in palette for c in up_colors)
    assert len(set(up_colors)) == 2
    assert {seg["op"] for seg in rendered["up"]} == {"1"}
    assert [seg["bg"] for seg in rendered["down"]] == up_colors
    assert {seg["op"] for seg in rendered["down"]} == {"0.55"}
    assert rendered["legend"] == up_colors
    # Distinct per-stage scroll key.
    assert rendered["scrollKey"] == "validate-model"


def test_generate_model_chart_stacks_success_up_and_failure_down_by_model() -> None:
    model = {
        "models": ["glm-5.2-pretrain-v1", "deepseek-v4-flash"],
        "buckets": [
            {
                "t": "2026-08-07T10:00:00Z",
                "by_model": {
                    "glm-5.2-pretrain-v1": {"succeeded": 10, "failed": 2, "rejected": 0},
                    "deepseek-v4-flash": {"succeeded": 4, "failed": 6, "rejected": 0},
                },
            }
        ],
    }
    palette = _model_palette()
    rendered = render_diverging_model_chart(model, "generate")
    assert rendered["empty"] is False
    # Two model colours appear above the axis at full opacity (success). Colours
    # are hash-derived per model_id (not by array index), so assert they are two
    # distinct palette entries rather than fixed hexes.
    up_colors = [seg["bg"] for seg in rendered["up"]]
    assert all(c in palette for c in up_colors)
    assert len(set(up_colors)) == 2
    assert {seg["op"] for seg in rendered["up"]} == {"1"}
    # The SAME per-model colours appear below the axis, faded, for failures.
    assert [seg["bg"] for seg in rendered["down"]] == up_colors
    assert {seg["op"] for seg in rendered["down"]} == {"0.55"}
    # The legend maps each colour to a model_id in the same order.
    assert rendered["legend"] == up_colors
    assert rendered["scrollKey"] == "generate-model"


def test_diverging_chart_draws_rejected_as_a_thin_neutral_marker() -> None:
    # Rejected results are neither up nor down: they render as a thin neutral
    # marker on the down side and never inflate the failure segments.
    stage_data = {
        "models": ["glm-5.2-pretrain-v1"],
        "buckets": [
            {
                "t": "2026-08-07T10:00:00Z",
                "by_model": {
                    "glm-5.2-pretrain-v1": {"succeeded": 5, "failed": 1, "rejected": 4},
                },
            }
        ],
    }
    rendered = render_diverging_model_chart(stage_data, "validate")
    # One up segment (success), one down segment (failed), one rejected marker.
    assert len(rendered["up"]) == 1
    assert len(rendered["down"]) == 1
    assert rendered["rejected"] == 1


def test_diverging_chart_tooltip_is_a_compact_multiline_table() -> None:
    # The per-bucket tooltip is a short vertical table, not one wide middle-dot
    # line, so it no longer overflows on hover: a header carrying the up-total and
    # column labels, then one `model: succeeded|failed` line per active model.
    stage_data = {
        "models": ["deepseek-v4-flash", "glm-5.2-pretrain-v1"],
        "buckets": [
            {
                "t": "2026-08-07T10:00:00Z",
                "by_model": {
                    "deepseek-v4-flash": {"succeeded": 60, "failed": 20, "rejected": 0},
                    "glm-5.2-pretrain-v1": {"succeeded": 100, "failed": 20, "rejected": 0},
                },
            }
        ],
    }
    rendered = render_diverging_model_chart(stage_data, "generate")
    tooltip = rendered["tooltips"][0]
    lines = tooltip.split("\n")
    # Multi-line, not the old "·"-joined single line.
    assert "\n" in tooltip
    assert " · " not in tooltip
    # Header labels the two columns and carries the bucket's success (up) total.
    assert lines[1] == "total 160 - success|failed"
    # One short line per active model, `model: succeeded|failed`.
    assert "deepseek-v4-flash: 60|20" in lines
    assert "glm-5.2-pretrain-v1: 100|20" in lines


def test_diverging_chart_tooltip_shows_rejected_only_as_a_third_column() -> None:
    # Rejected is folded into the table as a third column (header and per-model),
    # only when the bucket actually has rejections, keeping the common case short.
    stage_data = {
        "models": ["glm-5.2-pretrain-v1"],
        "buckets": [
            {
                "t": "2026-08-07T10:00:00Z",
                "by_model": {
                    "glm-5.2-pretrain-v1": {"succeeded": 5, "failed": 1, "rejected": 4},
                },
            }
        ],
    }
    rendered = render_diverging_model_chart(stage_data, "validate")
    lines = rendered["tooltips"][0].split("\n")
    assert lines[1] == "total 5 - success|failed|rejected"
    assert "glm-5.2-pretrain-v1: 5|1|4" in lines


def test_generate_model_chart_renders_empty_state_without_models() -> None:
    rendered = render_diverging_model_chart({"models": [], "buckets": []}, "push")
    assert rendered["empty"] is True
    assert rendered["up"] == [] and rendered["down"] == []


def _diverging_chart_program(body: str) -> str:
    """The chart renderer + updater under the shared DOM stub, plus `body`."""

    from swegen.dashboard.server import HTML

    consts = "".join(
        re.search(re.escape(prefix) + r".*?;\n", HTML).group(0)
        for prefix in (
            "const MODEL_PALETTE=",
            "const modelColorIndex=",
            "const modelColor=",
            "const compactChartCount=",
            "const compactChartTimestamp=",
        )
    )
    renderer = re.search(
        r"function rescaleVisible\(chart\)\{.*?\n(?=function validateQueueLine)",
        HTML,
        re.S,
    )
    assert renderer is not None
    return _DOM_STUB + consts + renderer.group(0) + body


def update_diverging_chart(
    first: object,
    second: object,
    stage: str = "generate",
) -> dict[str, object]:
    """Render a chart, then refresh it with `second` via updateDivergingChart.

    Returns whether the same wrap/cell/segment object identities survived the
    refresh (the anti-flash invariant) alongside the updated values, so a test can
    prove the DOM was mutated rather than rebuilt.
    """

    program = _diverging_chart_program(
        f"const first={json.dumps(first)};const second={json.dumps(second)};"
        + f"const stage={json.dumps(stage)};"
        + r"""
const wrap=divergingModelTimeSeries(first,stage,48);
const chart=wrap._chart;
const cellsBefore=chart._buckets.map(r=>r.cell);
const upSegsBefore=chart._buckets.map(r=>r.upSegs.map(s=>s.seg));
const next=updateDivergingChart(wrap,second,stage,48);
const sameWrap=next===wrap;
const sameChart=next._chart===chart;
const sameCells=chart._buckets.every((r,i)=>r.cell===cellsBefore[i]);
const sameSegs=chart._buckets.every((r,i)=>r.upSegs.every((s,j)=>s.seg===upSegsBefore[i][j]));
/* Read the state off the RESULTING chart: identical to the original when updated
in place, and the freshly built one when the shape changed. */
const out=next._chart,recs=out._buckets;
process.stdout.write(JSON.stringify({
  sameWrap,sameChart,sameCells,sameSegs,
  ups:recs.map(r=>r.up),
  downs:recs.map(r=>r.down),
  upSegValues:recs.map(r=>r.upSegs.map(s=>s.value)),
  upSegColors:recs.map(r=>r.upSegs.map(s=>s.seg.style.background)),
  tooltips:recs.map(r=>r.tooltip),
  ariaLabels:recs.map(r=>r.cell.attrs['aria-label']),
  rejectedHidden:recs.map(r=>r.rejectedMarker.hidden),
  ticks:[...out._yAxis.children].map(t=>t.textContent),
}));
"""
    )
    completed = run(["node", "-e", program], check=True, capture_output=True, text=True)
    return json.loads(completed.stdout)


def _bucket(timestamp: str, counts: dict[str, dict[str, int]]) -> dict[str, object]:
    return {"t": timestamp, "by_model": counts}


def test_chart_refresh_mutates_the_existing_dom_instead_of_rebuilding_it() -> None:
    """The anti-flash invariant: a same-shape poll reuses every chart node.

    Tearing the chart down and re-creating it on each 5s poll is what made the
    stacked bars visibly flash and transiently paint other models' colours. With
    the bucket count and model set unchanged, the refresh must mutate the existing
    wrap/cells/segments in place and keep the same object identities.
    """

    models = ["glm-5.2-pretrain-v1", "deepseek-v4-flash"]
    first = {
        "models": models,
        "buckets": [
            _bucket(
                "2026-08-07T10:00:00Z",
                {
                    "glm-5.2-pretrain-v1": {"succeeded": 10, "failed": 2, "rejected": 0},
                    "deepseek-v4-flash": {"succeeded": 4, "failed": 6, "rejected": 0},
                },
            )
        ],
    }
    # Same bucket count, same model set, new counts -> in-place update.
    second = {
        "models": models,
        "buckets": [
            _bucket(
                "2026-08-07T10:00:00Z",
                {
                    "glm-5.2-pretrain-v1": {"succeeded": 30, "failed": 5, "rejected": 0},
                    "deepseek-v4-flash": {"succeeded": 6, "failed": 9, "rejected": 0},
                },
            )
        ],
    }
    result = update_diverging_chart(first, second)

    # Not one node was replaced.
    assert result["sameWrap"] is True
    assert result["sameChart"] is True
    assert result["sameCells"] is True
    assert result["sameSegs"] is True
    # The numbers did update, in place.
    assert result["ups"] == [36]
    assert result["downs"] == [14]
    assert result["upSegValues"] == [[30, 6]]
    # Model colours are unchanged by the refresh (two distinct palette entries).
    palette = _model_palette()
    colors = result["upSegColors"][0]
    assert all(c in palette for c in colors)
    assert len(set(colors)) == 2
    # The y-axis ticks were rescaled from the new totals by rescaleVisible.
    assert result["ticks"] == ["36", "0", "-14"]
    # Tooltips (and their aria-labels) were refreshed in place too.
    assert "glm-5.2-pretrain-v1: 30|5" in result["tooltips"][0]
    assert result["ariaLabels"][0] == result["tooltips"][0]


def test_chart_refresh_rebuilds_only_when_the_bucket_count_or_models_change() -> None:
    """A shape change is the one case that still needs a fresh chart."""

    models = ["glm-5.2-pretrain-v1"]
    counts = {"glm-5.2-pretrain-v1": {"succeeded": 5, "failed": 1, "rejected": 0}}
    one_bucket = {"models": models, "buckets": [_bucket("2026-08-07T10:00:00Z", counts)]}
    # A new 15m bucket rolls in -> bucket count changed -> rebuild.
    two_buckets = {
        "models": models,
        "buckets": [
            _bucket("2026-08-07T10:00:00Z", counts),
            _bucket("2026-08-07T10:15:00Z", counts),
        ],
    }
    grew = update_diverging_chart(one_bucket, two_buckets)
    assert grew["sameWrap"] is False
    assert grew["ups"] == [5, 5]

    # A new model appears -> model set changed -> rebuild.
    more_models = {
        "models": [*models, "deepseek-v4-flash"],
        "buckets": [
            _bucket(
                "2026-08-07T10:00:00Z",
                {**counts, "deepseek-v4-flash": {"succeeded": 3, "failed": 0, "rejected": 0}},
            )
        ],
    }
    remodelled = update_diverging_chart(one_bucket, more_models)
    assert remodelled["sameWrap"] is False
    assert remodelled["upSegValues"] == [[5, 3]]


def test_chart_refresh_toggles_the_rejected_marker_without_rebuilding() -> None:
    # The rejected marker is always present and toggled via `hidden`, so a bucket
    # gaining or losing rejections never adds/removes a node mid-paint.
    models = ["glm-5.2-pretrain-v1"]
    without = {
        "models": models,
        "buckets": [
            _bucket(
                "2026-08-07T10:00:00Z",
                {"glm-5.2-pretrain-v1": {"succeeded": 5, "failed": 1, "rejected": 0}},
            )
        ],
    }
    with_rejected = {
        "models": models,
        "buckets": [
            _bucket(
                "2026-08-07T10:00:00Z",
                {"glm-5.2-pretrain-v1": {"succeeded": 5, "failed": 1, "rejected": 4}},
            )
        ],
    }
    appeared = update_diverging_chart(without, with_rejected)
    assert appeared["sameWrap"] is True
    assert appeared["rejectedHidden"] == [False]
    assert appeared["tooltips"][0].endswith("glm-5.2-pretrain-v1: 5|1|4")

    disappeared = update_diverging_chart(with_rejected, without)
    assert disappeared["sameWrap"] is True
    assert disappeared["rejectedHidden"] == [True]


def test_stage_flow_updates_cards_in_place_without_wiping_the_container() -> None:
    from swegen.dashboard.server import HTML

    # The stages container is populated once and then left alone: renderStageFlow
    # must not open with a bare replaceChildren() that destroys every card (and
    # its charts) on every poll.
    assert "const flow=el('stages');flow.replaceChildren();" not in HTML
    # Cached cards live on uiState and are refreshed in place on later polls.
    assert "const cards=uiState.stageCards" in HTML
    assert "if(cards&&flow.firstChild)" in HTML
    assert "stages.forEach(stage=>updateStageCard(cards[stage],stage,pg,k,maxReplicas))" in HTML
    assert "function updateStageCard(card,stage,pg,k,maxReplicas)" in HTML
    # The in-place path rewrites the stat text and delegates chart/yield refresh
    # to their own updaters rather than re-creating the card.
    assert "applyStageStatLines(card,stageStatLines(stage,pg,k))" in HTML
    assert "function updateDivergingChart(wrap,stageData,stage,rangeHours)" in HTML
    assert "function updateStageHourlyYield(wrap,rows,stage)" in HTML
    # A chart/yield node is only swapped when its shape actually changed.
    assert "if(nextChart!==card._chartWrap)" in HTML
    assert "if(nextYield!==card._yieldWrap)" in HTML


def test_poll_loop_is_self_scheduling_and_never_overlaps() -> None:
    from swegen.dashboard.server import HTML

    # setInterval fired every 5s regardless of whether the previous request had
    # returned; with 4-7s responses that queued overlapping requests and
    # compounded the latency. The loop now re-arms only after a response settles.
    # No setInterval call survives (the only mention left is the comment
    # explaining why it was replaced, so match the call form).
    assert "setInterval(" not in HTML.replace("setInterval(poll,5000) fired", "")
    assert "setTimeout(pollLoop,POLL_INTERVAL_MS)" in HTML
    assert "async function pollLoop(){await poll();scheduleNextPoll()}" in HTML
    # A guard flag makes a re-entrant poll a no-op, so a slow response can never
    # stack a second in-flight fetch.
    assert "if(uiState.polling)return;uiState.polling=true" in HTML
    assert "finally{uiState.polling=false}" in HTML
    # Boot goes through the loop, not a bare poll + interval pair.
    assert "\npollLoop();\n" in HTML
    # The cadence is measured from response completion, so the stamp no longer
    # promises a fixed 5s refresh.
    assert "refreshes every 5s" not in HTML
    assert "refreshes ${POLL_INTERVAL_MS/1000}s after each response" in HTML


def test_poll_cadence_defaults_to_15s_and_is_injected_from_the_server(monkeypatch) -> None:
    """Browser and server share one cadence, defaulting to 15s, not 5s.

    A 5s dashboard poll was on its own enough to take the k3s API server from
    passing 4/4 /livez probes to failing 3/3 against ~2,400 pods; it recovered
    within 30s of the dashboard being stopped. The browser value is templated
    from the server's own refresh interval so the two cannot drift, and a
    browser polling faster than the snapshot behind it changes is pure waste.
    """

    from swegen.dashboard.server import (
        DASHBOARD_REFRESH_SECONDS_DEFAULT,
        DASHBOARD_REFRESH_SECONDS_ENV,
        HTML,
        SnapshotCache,
    )

    assert DASHBOARD_REFRESH_SECONDS_DEFAULT == 15.0
    # The literal is templated, never hardcoded in the page source.
    assert "const POLL_INTERVAL_MS=__POLL_INTERVAL_MS__;" in HTML
    assert "const POLL_INTERVAL_MS=5000" not in HTML

    monkeypatch.delenv(DASHBOARD_REFRESH_SECONDS_ENV, raising=False)
    assert SnapshotCache().refresh_seconds == 15.0

    monkeypatch.setenv(DASHBOARD_REFRESH_SECONDS_ENV, "30")
    assert SnapshotCache().refresh_seconds == 30.0

    # A typo'd or non-positive override falls back rather than busy-looping.
    monkeypatch.setenv(DASHBOARD_REFRESH_SECONDS_ENV, "0")
    assert SnapshotCache().refresh_seconds == 15.0
    monkeypatch.setenv(DASHBOARD_REFRESH_SECONDS_ENV, "soon")
    assert SnapshotCache().refresh_seconds == 15.0

    # An explicit argument still wins, so tests and callers can pin it.
    assert SnapshotCache(refresh_seconds=2.0).refresh_seconds == 2.0


def test_index_page_renders_the_polling_interval_from_the_cache_cadence() -> None:
    """The served page carries a real number, never the unsubstituted token."""

    from swegen.dashboard.server import HTML, SnapshotCache

    cache = SnapshotCache(refresh_seconds=15.0)
    body = HTML.replace("__CSRF_TOKEN__", "tok").replace(
        "__POLL_INTERVAL_MS__", str(int(max(1.0, cache.refresh_seconds) * 1000))
    )

    assert "const POLL_INTERVAL_MS=15000;" in body
    assert "__POLL_INTERVAL_MS__" not in body


def test_node_disk_io_cell_reports_breaker_state_and_sample_age() -> None:
    """A frozen cAdvisor sample must read as frozen, not as live numbers.

    The node cAdvisor endpoint stopped completing at ~600 pods/node, so the
    dashboard kept rendering whatever it last had. The cell now says either
    "disk I/O unavailable — polling suspended after N consecutive failures" or
    "... · from 4 minutes ago".
    """

    from swegen.dashboard.server import HTML

    assert "function diskIoLabel(io)" in HTML
    assert "polling suspended after ${breaker.consecutive_failures} consecutive failures" in HTML
    assert "disk I/O unavailable" in HTML
    assert "from ${ageLabel(io.age_seconds)}" in HTML
    # The age comes from the payload's server-measured seconds, not from parsing
    # a timestamp against a browser clock that may disagree.
    assert "const ageLabel=s=>" in HTML
    assert "diskCell.title=diskView.detail" in HTML


def render_stage_hourly_yield(rows: object, stage: str = "generate") -> dict[str, object]:
    from swegen.dashboard.server import HTML

    renderer = re.search(
        r"function stageHourlyYield\(rows,stage\)\{.*?\n(?=function )",
        HTML,
        re.S,
    )
    assert renderer is not None
    program = (
        _DOM_STUB
        + renderer.group(0)
        + f"const wrap=stageHourlyYield({json.dumps(rows)},{json.dumps(stage)});"
        + r"""
let rowsOut=[],title=null,note=null,empty=null;
(function walk(node){const cls=node.className||'';
  if(cls==='stage-yield-title'){const spans=(node.children||[]).map(c=>c.textContent);title=spans[0];note=spans[1];}
  if(cls==='yield-empty')empty=node.textContent;
  if(cls==='yield-row'){const spans=(node.children||[]).map(c=>c.textContent);rowsOut.push({stamp:spans[0],value:spans[1],percent:spans[2]});}
  (node.children||[]).forEach(walk);})(wrap);
process.stdout.write(JSON.stringify({rootClass:wrap.className,title,note,rows:rowsOut,empty}));
"""
    )
    completed = run(
        ["node", "-e", program], check=True, capture_output=True, text=True
    )
    return json.loads(completed.stdout)


def test_stage_hourly_yield_renders_a_compact_per_stage_mini_panel() -> None:
    # The per-stage yield renderer returns a compact `.stage-yield` element (to be
    # placed beside the stage's chart), one row per hourly bucket with a
    # `succeeded / processed` value and a yield percent.
    rows = [
        {"bucket": "2026-08-07T09:00:00Z", "succeeded": 8, "processed": 10, "yield_percent": 80.0},
        {"bucket": "2026-08-07T10:00:00Z", "succeeded": 3, "processed": 12, "yield_percent": 25.0},
    ]
    rendered = render_stage_hourly_yield(rows, "validate")
    assert rendered["rootClass"] == "stage-yield"
    assert rendered["title"] == "Hourly yield · last 12h"
    assert rendered["note"] == "success / terminal"
    assert rendered["empty"] is None
    assert [r["value"] for r in rendered["rows"]] == ["8 / 10", "3 / 12"]
    assert [r["percent"] for r in rendered["rows"]] == ["80.0%", "25.0%"]


def test_stage_hourly_yield_degrades_to_an_empty_note_and_dashes_percent() -> None:
    # No buckets -> a compact empty state, not an exception.
    empty = render_stage_hourly_yield([], "push")
    assert empty["rows"] == []
    assert empty["empty"] == "No terminal outcomes in the last 12 hours"
    # A null yield percent (zero processed) renders as an em dash, never NaN.
    dashed = render_stage_hourly_yield(
        [{"bucket": "2026-08-07T10:00:00Z", "succeeded": 0, "processed": 0, "yield_percent": None}],
        "reward",
    )
    assert dashed["rows"][0]["percent"] == "—"
    assert dashed["rows"][0]["value"] == "0 / 0"


def test_stage_card_embeds_hourly_yield_beside_the_chart() -> None:
    from swegen.dashboard.server import HTML

    # The stage card reads its own hourly-yield slice and builds a mini-panel that
    # sits beside the timeseries chart, inside each card.
    assert "pg.hourly_yield?.stages?.[stage]||[]" in HTML
    assert "const yieldView=stageHourlyYield(yieldRows,stage)" in HTML
    assert "function stageHourlyYield(rows,stage)" in HTML
    # A dedicated compact yield column styled like the chart column.
    assert ".stage-card-horizontal .stage-yield{border-left:1px solid var(--line)" in HTML


def test_top_stage_cards_separate_fresh_activity_from_queue_leases() -> None:
    from swegen.dashboard.server import HTML

    assert "pg.activity?.stages?.[stage]" in HTML
    assert "· active ${a.fresh||0}" in HTML
    assert "leased ${q.in_flight||0}" in HTML
    assert "stale ${stale}" in HTML


def test_validator_card_shows_repaired_and_brand_new_queue_totals_and_leases() -> None:
    from swegen.dashboard.server import HTML

    assert "Freshly repaired" in HTML
    assert "Brand-new tasks" in HTML
    assert "pg.queues?.validate_repaired" in HTML
    assert "pg.queues?.validate_new" in HTML
    assert "total <b></b> · ready <b></b> · leased/in-flight <b></b>" in HTML
    assert "queue?.length??'—'" in HTML
    assert "queue?.visible??'—'" in HTML
    assert "queue?.in_flight??'—'" in HTML


def test_top_stage_cards_show_pod_phases_instead_of_a_ready_fraction() -> None:
    from swegen.dashboard.server import HTML

    assert "running:`${w.pod_phases?.Running||0} Running`" in HTML
    assert "formatPodPhases(w.pod_phases,w.evicted||0)" in HTML
    assert "queueSummary:`desired ${w.desired||0}" in HTML
    assert "${w.ready||0}/${w.desired||0} ready" not in HTML
    assert "w.pods_ready" not in HTML


def test_pod_phase_line_lists_non_running_states_and_buckets_evictions() -> None:
    phases = {"Running": 122, "Pending": 6, "Terminating": 2, "Succeeded": 12}

    assert evaluate_pod_phases(phases, 12_474) == (
        "Pending 6 · Terminating 2 · Succeeded 12 · Evicted 12474"
    )
    assert evaluate_pod_phases({"Running": 160}, 0) == "no other pod states"
    assert evaluate_pod_phases({}, 0) == "no other pod states"
    assert evaluate_pod_phases(None, 0) == "no other pod states"
    # A scaled-to-zero stage with orphaned activity rows must read as zero pods, not "0/0 ready".
    assert evaluate_pod_phases({}, 54) == "Evicted 54"


def test_validation_and_repair_share_a_visual_retry_group_without_arrows() -> None:
    from swegen.dashboard.server import HTML

    assert "validationLoop.className='validation-loop'" in HTML
    assert "stageCard('validate',pg,k,maxReplicas,true)" in HTML
    assert "stageCard('repair',pg,k,maxReplicas,true)" in HTML
    assert "Validation / repair retry group" in HTML
    assert "flowArrow" not in HTML
    assert "→" not in HTML
    assert "↑" not in HTML
    assert "↓" not in HTML


def test_top_stage_flow_keeps_reward_after_the_validation_group() -> None:
    from swegen.dashboard.server import HTML

    assert "stageCard('generate',pg,k,maxReplicas,true)" in HTML
    assert "flow.replaceChildren(built.generate,validationLoop,built.reward,built.push)" in HTML
    assert "validationLoop.append(groupTitle,built.validate,built.repair)" in HTML
    assert "stageCard('push',pg,k,maxReplicas,true)" in HTML


def test_all_five_stage_cards_use_the_three_column_horizontal_layout() -> None:
    from swegen.dashboard.server import HTML

    # Every main stage card must be wired to the 3-column horizontal layout by
    # passing horizontalChart=true, so generate/validate/repair/reward/push all
    # render [stats | chart | hourly-yield] side by side.
    assert "stageCard('generate',pg,k,maxReplicas,true)" in HTML
    assert "stageCard('validate',pg,k,maxReplicas,true)" in HTML
    assert "stageCard('repair',pg,k,maxReplicas,true)" in HTML
    assert "stageCard('reward',pg,k,maxReplicas,true)" in HTML
    assert "stageCard('push',pg,k,maxReplicas,true)" in HTML
    # No stage is rendered without the horizontal flag (the old vertical variant).
    assert "stageCard('generate',pg,k,maxReplicas)" not in HTML
    assert "stageCard('reward',pg,k,maxReplicas)" not in HTML
    assert "stageCard('push',pg,k,maxReplicas))" not in HTML
    # The three-column grid CSS applies to the stage cards.
    assert (
        ".stage-card-horizontal{display:grid;"
        "grid-template-columns:240px minmax(0,1fr) minmax(150px,210px)" in HTML
    )
    # The horizontal branch appends stats, chart, and yield directly.
    assert "if(horizontalChart){card.append(stats,chart,yieldView)}" in HTML
    # The vertical single-column stacking of full-width cards still holds.
    assert ".pipeline-flow{display:grid;grid-template-columns:1fr" in HTML
    assert ".pipeline-flow>.stage-card{min-width:0}" in HTML


def test_nested_validation_stage_cards_preserve_metrics_and_controls() -> None:
    from swegen.dashboard.server import HTML

    assert "function stageCard(stage,pg,k,maxReplicas,horizontalChart=false)" in HTML
    # validate/repair are single-pool stages, so they still get the stage-wide
    # spinner (it now lives in the non-generate branch; see
    # test_stage_card_branches_on_generate_for_the_endpoints_panel).
    assert "stats.append(scaleControls(stage,lines.desired,maxReplicas))" in HTML
    assert "throughput:`5m success ${t.succeeded||0}" in HTML
    assert "lifetime:`lifetime processed ${lifetime}`" in HTML


def test_stage_scaling_buttons_use_the_compact_apply_label() -> None:
    from swegen.dashboard.server import HTML

    assert "setText(button,'Apply')" in HTML
    assert "Apply configuration" not in HTML


def test_worker_spinner_control_is_narrow_centered_and_responsive() -> None:
    from swegen.dashboard.server import HTML

    assert ".scale-controls{width:100%;max-width:174px" in HTML
    assert "grid-template-columns:minmax(76px,1fr) 58px" in HTML
    assert "margin:8px auto 0" in HTML
    assert ".scale-limit{" in HTML and "text-align:center" in HTML


def test_stage_charts_are_embedded_with_the_requested_placements() -> None:
    from swegen.dashboard.server import HTML

    # Horizontal cards lay out [stats | chart | hourly-yield] in one grid; the
    # vertical cards wrap the chart and yield in a side-by-side row below stats.
    assert "card.append(stats,chart,yieldView)" in HTML
    assert "chartRow.append(chart,yieldView);card.append(stats,chartRow)" in HTML
    # Every stage routes to the shared diverging per-model chart, reading its
    # slice of the unified stage_model_timeseries structure.
    assert (
        "divergingModelTimeSeries(pg.stage_model_timeseries?.stages?.[stage],stage,"
        "pg.stage_model_timeseries?.lookback_hours)" in HTML
    )
    assert "stageCard('validate',pg,k,maxReplicas,true)" in HTML
    assert "stageCard('repair',pg,k,maxReplicas,true)" in HTML
    # The horizontal card is now a three-column grid: stats, chart, yield.
    assert (
        ".stage-card-horizontal{display:grid;"
        "grid-template-columns:240px minmax(0,1fr) minmax(150px,210px)" in HTML
    )
    assert (
        ".stage-card:not(.stage-card-horizontal) .stage-chart-row{flex:1;margin-top:10px" in HTML
    )
    assert ".stage-card-horizontal .stage-chart-wrap{border-left:1px solid var(--line)" in HTML
    assert ".stage-card{display:flex;flex-direction:column;padding:11px}" in HTML
    assert ".validation-loop{" in HTML and "align-content:stretch" in HTML
    assert "15m outcomes by model · last ${hours}h" in HTML


def test_stage_charts_grow_without_centering_margins() -> None:
    from swegen.dashboard.server import HTML

    assert ".stage-chart-wrap{min-width:0;min-height:0;display:flex;flex-direction:column}" in HTML
    assert ".stage-card:not(.stage-card-horizontal) .stage-chart-row{flex:1" in HTML
    assert ".stage-card:not(.stage-card-horizontal) .stage-chart-row .stage-chart-wrap{flex:1}" in HTML
    assert ".chart-frame{min-width:0;min-height:112px;flex:1" in HTML
    # position:relative makes the chart the offsetParent of its buckets so the
    # visible-window maths shares a coordinate frame with scrollLeft; the sizing
    # properties this test cares about are unchanged alongside it.
    assert ".chart{position:relative;min-height:112px;min-width:0" in HTML
    # Bar heights now go through the shared applyBucketHeights writer rather than
    # being inlined at the render site (same value->height mapping, one place).
    assert "c.upBar.style.height=`${Math.min(100,upPercent)}%`" in HTML
    assert "justify-content:center;padding:11px" not in HTML


def test_horizontal_validation_stats_are_fixed_width_and_left_aligned() -> None:
    from swegen.dashboard.server import HTML

    assert ".stage-card-horizontal .stage-stats{width:240px;text-align:left" in HTML
    assert "justify-self:start;align-self:start" in HTML


def test_generate_scales_a_single_deployment() -> None:
    from swegen.dashboard.server import K3sScaler

    # Generate is one deployment scaled directly, like every other stage: no
    # primary/overflow split, no 92-replica cap.
    assert K3sScaler.plan("generate", 96, max_replicas=768) == [
        ("swegen-generate", 96),
    ]
    assert K3sScaler.plan("generate", 80, max_replicas=768) == [
        ("swegen-generate", 80),
    ]


@pytest.mark.parametrize(
    ("stage", "replicas"),
    [
        ("generate; delete pods", 1),
        ("validate", True),
        ("reward", 1.5),
        ("push", -1),
        ("push", 769),
    ],
)
def test_scaler_rejects_unknown_stages_and_invalid_bounds(
    stage: object,
    replicas: object,
) -> None:
    from swegen.dashboard.server import K3sScaler

    with pytest.raises(ValueError):
        K3sScaler.plan(stage, replicas, max_replicas=768)


def test_scaler_accepts_the_dynamic_cluster_cpu_ceiling() -> None:
    from swegen.dashboard.server import K3sScaler

    assert K3sScaler.plan("reward", 768, max_replicas=768) == [("swegen-reward", 768)]
    with pytest.raises(ValueError, match="between 0 and 768"):
        K3sScaler.plan("reward", 769, max_replicas=768)


def test_scaling_controls_accept_2048_and_still_reject_just_above_it() -> None:
    # The raised ceiling must hold on BOTH write paths: stage scaling and
    # per-endpoint generate concurrency (single and bulk).
    from swegen.dashboard.distributed_status import MAX_SCALE_REPLICAS_DEFAULT
    from swegen.dashboard.server import K3sScaler, _validate_concurrency

    cap = MAX_SCALE_REPLICAS_DEFAULT
    assert cap == 2048

    assert K3sScaler.plan("generate", cap, max_replicas=cap) == [("swegen-generate", cap)]
    with pytest.raises(ValueError, match="between 0 and 2048"):
        K3sScaler.plan("generate", cap + 1, max_replicas=cap)

    assert _validate_concurrency(cap, max_replicas=cap) == cap
    with pytest.raises(ValueError, match="between 0 and 2048"):
        _validate_concurrency(cap + 1, max_replicas=cap)

    conn = _FakeEndpointConn()
    conn.rows["m1-alpha"] = "model-one"
    registry = _registry_with(conn)
    assert registry.scale(slug="m1-alpha", concurrency=cap, max_replicas=cap) == {
        "ok": True,
        "slug": "m1-alpha",
        "concurrency": cap,
    }
    assert registry.scale_many(
        items=[{"slug": "m1-alpha", "concurrency": cap}], max_replicas=cap
    )["applied"] == [{"slug": "m1-alpha", "concurrency": cap}]
    with pytest.raises(ValueError, match="between 0 and 2048"):
        registry.scale_many(
            items=[{"slug": "m1-alpha", "concurrency": cap + 1}], max_replicas=cap
        )


def test_raising_the_ceiling_did_not_loosen_the_type_or_sign_checks() -> None:
    # We raised a ceiling, not the validation. Bools, non-ints, floats and
    # negatives are still rejected at the new cap.
    from swegen.dashboard.server import SCALE_MIN, K3sScaler, _validate_concurrency

    assert SCALE_MIN == 0
    for bad in (True, False, "2048", 1.5, None, [1]):
        with pytest.raises(ValueError, match="must be an integer"):
            _validate_concurrency(bad, max_replicas=2048)
        with pytest.raises(ValueError, match="must be an integer"):
            K3sScaler.plan("generate", bad, max_replicas=2048)
    with pytest.raises(ValueError, match="between 0 and 2048"):
        _validate_concurrency(-1, max_replicas=2048)
    # Zero remains valid: scaling a pool down to nothing is a legitimate action.
    assert _validate_concurrency(0, max_replicas=2048) == 0


def test_max_scale_replicas_is_env_overridable_and_ignores_bad_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from swegen.dashboard.distributed_status import (
        MAX_SCALE_REPLICAS_DEFAULT,
        MAX_SCALE_REPLICAS_ENV,
        resolve_max_scale_replicas,
    )

    monkeypatch.delenv(MAX_SCALE_REPLICAS_ENV, raising=False)
    assert resolve_max_scale_replicas() == MAX_SCALE_REPLICAS_DEFAULT == 2048

    monkeypatch.setenv(MAX_SCALE_REPLICAS_ENV, "4096")
    assert resolve_max_scale_replicas() == 4096

    # A typo'd override must not take the dashboard down, and must never pin the
    # ceiling to 0 (which would make every scale control reject all input).
    for bad in ("", "   ", "abc", "12.5", "0", "-5"):
        monkeypatch.setenv(MAX_SCALE_REPLICAS_ENV, bad)
        assert resolve_max_scale_replicas() == MAX_SCALE_REPLICAS_DEFAULT


def test_scaler_uses_allowlisted_kubectl_argument_arrays() -> None:
    from swegen.dashboard.server import K3sScaler

    commands: list[list[str]] = []

    def runner(command: list[str], **_kwargs: object) -> CompletedProcess[str]:
        commands.append(command)
        return CompletedProcess(command, 0, stdout="scaled", stderr="")

    applied = K3sScaler(runner=runner).scale(
        "generate",
        96,
        max_replicas=768,
    )

    # Generate scales its single deployment directly: one argv-only kubectl
    # scale, no image/secret sync (that copy reverted the fleet to a stale
    # image, which is why the overflow pool was removed).
    assert applied == [
        {"deployment": "swegen-generate", "replicas": 96},
    ]
    assert commands == [
        [
            "kubectl",
            "--request-timeout=10s",
            "-n",
            "swegen-pipeline",
            "scale",
            "deployment/swegen-generate",
            "--replicas=96",
        ],
    ]
    assert not any("set" in command or "get" in command for command in commands)


def test_build_slot_controller_uses_snapshot_allowlist_and_atomic_exec() -> None:
    from swegen.dashboard.server import K3sBuildSlotController

    commands: list[list[str]] = []

    def runner(command: list[str], **_kwargs: object) -> CompletedProcess[str]:
        commands.append(command)
        return CompletedProcess(command, 0, stdout='{"slots":48}\n', stderr="")

    nodes = [
        {
            "name": "node-a",
            "build_slot_max": 192,
            "build_slot_probe_pod": "swegen-buildkit-pruner-a",
        }
    ]
    applied = K3sBuildSlotController(runner=runner).update(
        "node-a",
        48,
        nodes=nodes,
    )

    assert applied == {
        "node": "node-a",
        "slots": 48,
        "max_slots": 192,
        "controller_pod": "swegen-buildkit-pruner-a",
    }
    assert commands[0][:7] == [
        "kubectl",
        "--request-timeout=10s",
        "-n",
        "swegen-pipeline",
        "exec",
        "swegen-buildkit-pruner-a",
        "--",
    ]
    assert commands[0][-1] == "48"
    assert "os.replace(tmp,d/'count')" in commands[0][-2]
    assert "touch(exist_ok=True)" in commands[0][-2]


@pytest.mark.parametrize(
    ("node", "slots", "message"),
    [
        ("unknown", 32, "unknown node"),
        ("node-a", 0, "between 1 and 192"),
        ("node-a", 193, "between 1 and 192"),
        ("node-a", True, "slots must be an integer"),
    ],
)
def test_build_slot_controller_rejects_unallowlisted_or_invalid_updates(
    node: object,
    slots: object,
    message: str,
) -> None:
    from swegen.dashboard.server import K3sBuildSlotController

    nodes = [
        {
            "name": "node-a",
            "build_slot_max": 192,
            "build_slot_probe_pod": "swegen-buildkit-pruner-a",
        }
    ]
    with pytest.raises(ValueError, match=message):
        K3sBuildSlotController.plan(node, slots, nodes=nodes)


def test_build_slot_controller_rejects_node_without_controller_pod() -> None:
    from swegen.dashboard.server import K3sBuildSlotController

    with pytest.raises(ValueError, match="controller is unavailable"):
        K3sBuildSlotController.plan(
            "node-a",
            32,
            nodes=[{"name": "node-a", "build_slot_max": 192}],
        )


def test_cache_retains_last_good_capacity_without_dropping_below_configured() -> None:
    from swegen.dashboard.server import SnapshotCache

    class HealthyCollector:
        def collect(self) -> dict[str, object]:
            return {"ok": True}

    class FailedCollector:
        def collect(self) -> dict[str, object]:
            raise RuntimeError("k3s temporarily unavailable")

    cache = SnapshotCache()
    cache._snapshot = {
        "postgres": {},
        "k3s": {
            "scaling": {"max_replicas": 768, "stale": False},
            "stages": {"validate": {"desired": 800}},
        },
        "sources": {},
    }
    cache._collectors = {
        "postgres": HealthyCollector(),
        "k3s": FailedCollector(),
    }

    cache.refresh()

    scaling = cache.snapshot()["k3s"]["scaling"]
    assert scaling["max_replicas"] == 800
    assert scaling["stale"] is True


class _FakeResult:
    def __init__(self, rowcount: int = 0, rows: list | None = None) -> None:
        self.rowcount = rowcount
        self._rows = rows or []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class _FakeEndpointConn:
    """In-memory stand-in for an autocommit psycopg connection."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple]] = []
        self.rows: dict[str, str] = {}
        self.events: list[tuple] = []
        # Optional richer per-slug endpoint fields used by the reset probe path.
        # Maps slug -> {"base_url", "auth_token", "breaker_open", "last_probe_status"}.
        self.endpoints: dict[str, dict] = {}

    def __enter__(self) -> _FakeEndpointConn:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def execute(self, sql: str, params: tuple = ()) -> _FakeResult:
        self.calls.append((sql, params))
        normalized = sql.strip().upper()
        if normalized.startswith("SELECT 1"):
            return _FakeResult(rows=[(1,)] if params[0] in self.rows else [])
        if normalized.startswith("SELECT MODEL_ID"):
            slug = params[0]
            return _FakeResult(rows=[(self.rows[slug],)] if slug in self.rows else [])
        if normalized.startswith("SELECT BASE_URL"):
            slug = params[0]
            if slug not in self.rows:
                return _FakeResult(rows=[])
            info = self.endpoints.get(slug, {})
            return _FakeResult(
                rows=[
                    (
                        info.get("base_url", "https://endpoint.example.com"),
                        self.rows[slug],
                        info.get("auth_token", "TOK"),
                    )
                ]
            )
        if normalized.startswith("INSERT INTO GENERATE_ENDPOINTS"):
            self.rows[params[0]] = params[2]
            return _FakeResult(rowcount=1)
        if normalized.startswith("INSERT INTO GENERATE_ENDPOINT_EVENTS"):
            self.events.append(params)
            return _FakeResult(rowcount=1)
        if normalized.startswith("UPDATE"):
            slug = params[-1]
            if slug in self.rows:
                info = self.endpoints.setdefault(slug, {})
                if "BREAKER_OPEN = FALSE" in normalized:
                    # Unlatch UPDATE: params = (status, now, slug).
                    info["breaker_open"] = False
                    info["last_probe_status"] = params[0]
                elif "LAST_PROBE_STATUS = %S" in normalized:
                    # Failed-probe UPDATE: params = (reason, status, now, slug).
                    info["last_probe_status"] = params[1]
            return _FakeResult(rowcount=1 if slug in self.rows else 0)
        if normalized.startswith("DELETE"):
            self.rows.pop(params[0], None)
            return _FakeResult(rowcount=1)
        return _FakeResult()


class _FakeProber:
    """Deterministic prober stand-in for the reset probe path."""

    def __init__(self, result) -> None:
        self.result = result
        self.calls: list[tuple[str, str, str]] = []

    def probe(self, base_url: str, model_id: str, token: str):
        self.calls.append((base_url, model_id, token))
        return self.result


def _registry_with(conn: _FakeEndpointConn, prober=None):
    from swegen.dashboard.server import GenerateEndpointRegistry

    return GenerateEndpointRegistry(connect=lambda: conn, prober=prober)


def test_endpoint_slug_derivation_is_k8s_label_safe() -> None:
    from swegen.dashboard.server import ENDPOINT_SLUG_RE, derive_endpoint_slug

    slug = derive_endpoint_slug("Claude/Opus 4.8", "https://Api.Example.com:8443/v1")
    assert ENDPOINT_SLUG_RE.match(slug)
    assert slug == slug.lower()
    assert not slug.startswith("-") and not slug.endswith("-")
    assert len(slug) <= 40


def test_register_endpoint_inserts_row_event_and_returns_slug() -> None:
    conn = _FakeEndpointConn()
    registry = _registry_with(conn)

    result = registry.register(
        base_url="https://alpha.example.com/v1",
        model_id="model-one",
        auth_token="TOP-SECRET-TOKEN",
        concurrency=8,
        max_replicas=100,
    )

    assert result["ok"] is True
    slug = result["slug"]
    assert slug in conn.rows
    # A registered event is written; its params never carry the token.
    assert any(params[2] == "registered" for params in conn.events)
    assert all("TOP-SECRET-TOKEN" not in str(params) for params in conn.events)
    # The token is not echoed back in the response.
    assert "TOP-SECRET-TOKEN" not in json.dumps(result)


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"base_url": "ftp://x", "model_id": "m", "auth_token": "t", "concurrency": 1}, "base_url"),
        ({"base_url": "not-a-url", "model_id": "m", "auth_token": "t", "concurrency": 1}, "base_url"),
        ({"base_url": "https://a.com", "model_id": "", "auth_token": "t", "concurrency": 1}, "model_id"),
        ({"base_url": "https://a.com", "model_id": "m", "auth_token": " ", "concurrency": 1}, "auth_token"),
        ({"base_url": "https://a.com", "model_id": "m", "auth_token": "t", "concurrency": -1}, "between 0"),
        ({"base_url": "https://a.com", "model_id": "m", "auth_token": "t", "concurrency": 101}, "between 0"),
    ],
)
def test_register_endpoint_rejects_invalid_input(kwargs: dict, match: str) -> None:
    registry = _registry_with(_FakeEndpointConn())
    with pytest.raises(ValueError, match=match):
        registry.register(max_replicas=100, **kwargs)


def test_register_endpoint_rejects_duplicate_slug() -> None:
    from swegen.dashboard.server import EndpointConflictError

    conn = _FakeEndpointConn()
    registry = _registry_with(conn)
    first = registry.register(
        base_url="https://alpha.example.com",
        model_id="model-one",
        auth_token="t",
        concurrency=1,
        max_replicas=100,
    )
    with pytest.raises(EndpointConflictError):
        registry.register(
            base_url="https://alpha.example.com",
            model_id="model-one",
            auth_token="t2",
            concurrency=1,
            max_replicas=100,
        )
    assert first["slug"] in conn.rows


def test_scale_endpoint_updates_and_writes_event() -> None:
    conn = _FakeEndpointConn()
    conn.rows["m1-alpha"] = "model-one"
    registry = _registry_with(conn)

    result = registry.scale(slug="m1-alpha", concurrency=20, max_replicas=100)

    assert result == {"ok": True, "slug": "m1-alpha", "concurrency": 20}
    assert any(params[2] == "scaled" for params in conn.events)


def test_scale_endpoint_rejects_out_of_range_concurrency() -> None:
    conn = _FakeEndpointConn()
    conn.rows["m1-alpha"] = "model-one"
    registry = _registry_with(conn)
    with pytest.raises(ValueError, match="between 0"):
        registry.scale(slug="m1-alpha", concurrency=999, max_replicas=100)


def test_scale_endpoint_missing_slug_raises_not_found() -> None:
    from swegen.dashboard.server import EndpointNotFoundError

    registry = _registry_with(_FakeEndpointConn())
    with pytest.raises(EndpointNotFoundError):
        registry.scale(slug="does-not-exist", concurrency=1, max_replicas=100)
    # A slug that is not even label-shaped is also a 404, not a 500.
    with pytest.raises(EndpointNotFoundError):
        registry.scale(slug="Bad Slug!", concurrency=1, max_replicas=100)


def test_scale_many_applies_every_row_and_writes_one_event_each() -> None:
    conn = _FakeEndpointConn()
    conn.rows["m1-alpha"] = "model-one"
    conn.rows["m2-beta"] = "model-two"
    conn.rows["m3-gamma"] = "model-three"
    registry = _registry_with(conn)

    result = registry.scale_many(
        items=[
            {"slug": "m1-alpha", "concurrency": 130},
            {"slug": "m2-beta", "concurrency": 0},
            {"slug": "m3-gamma", "concurrency": 7},
        ],
        max_replicas=200,
    )

    assert result["ok"] is True
    assert result["failed"] == []
    assert result["applied"] == [
        {"slug": "m1-alpha", "concurrency": 130},
        {"slug": "m2-beta", "concurrency": 0},
        {"slug": "m3-gamma", "concurrency": 7},
    ]
    # One 'scaled' audit event per applied row, carrying the new value.
    scaled = [params for params in conn.events if params[2] == "scaled"]
    assert len(scaled) == 3
    assert "concurrency set to 130" in scaled[0][3]
    assert "concurrency set to 0" in scaled[1][3]


def test_scale_many_reports_partial_failure_without_hiding_applied_rows() -> None:
    # The operator must never believe they set 3 rows when only 2 took. A slug
    # deleted between page load and Apply fails; the others still apply, and the
    # response names BOTH sets with ok=False.
    conn = _FakeEndpointConn()
    conn.rows["m1-alpha"] = "model-one"
    conn.rows["m3-gamma"] = "model-three"
    registry = _registry_with(conn)

    result = registry.scale_many(
        items=[
            {"slug": "m1-alpha", "concurrency": 5},
            {"slug": "m2-vanished", "concurrency": 9},
            {"slug": "m3-gamma", "concurrency": 11},
        ],
        max_replicas=200,
    )

    assert result["ok"] is False
    assert result["applied"] == [
        {"slug": "m1-alpha", "concurrency": 5},
        {"slug": "m3-gamma", "concurrency": 11},
    ]
    assert result["failed"] == [{"slug": "m2-vanished", "error": "endpoint not found"}]
    # The surviving rows really were written, and the missing row logged no event.
    assert [params[0] for params in conn.events if params[2] == "scaled"] == [
        "m1-alpha",
        "m3-gamma",
    ]


@pytest.mark.parametrize(
    ("items", "match"),
    [
        ([{"slug": "m1-alpha", "concurrency": 201}], "between 0"),
        ([{"slug": "m1-alpha", "concurrency": -1}], "between 0"),
        ([{"slug": "m1-alpha", "concurrency": True}], "must be an integer"),
        ([{"slug": "m1-alpha", "concurrency": "8"}], "must be an integer"),
        ([{"slug": "m1-alpha", "concurrency": 1.5}], "must be an integer"),
        ([{"slug": "m1-alpha"}], "must be an integer"),
        ([{"slug": "Bad Slug!", "concurrency": 1}], "not found"),
        (["not-an-object"], "each item"),
        ([], "non-empty"),
        ("not-a-list", "non-empty"),
    ],
)
def test_scale_many_rejects_invalid_input_before_writing_anything(
    items: object, match: str
) -> None:
    from swegen.dashboard.server import EndpointNotFoundError

    conn = _FakeEndpointConn()
    conn.rows["m1-alpha"] = "model-one"
    registry = _registry_with(conn)

    with pytest.raises((ValueError, EndpointNotFoundError), match=match):
        registry.scale_many(items=items, max_replicas=200)
    # Validation is a strict pre-pass: not a single UPDATE or event was written.
    assert conn.events == []
    assert not any(sql.strip().upper().startswith("UPDATE") for sql, _ in conn.calls)


def test_scale_many_validates_the_whole_batch_before_applying_any_row() -> None:
    # One bad value in the middle rejects the WHOLE batch: the valid rows around
    # it must not be half-applied, or the operator sees a partially-moved fleet.
    conn = _FakeEndpointConn()
    conn.rows["m1-alpha"] = "model-one"
    conn.rows["m2-beta"] = "model-two"
    registry = _registry_with(conn)

    with pytest.raises(ValueError, match="between 0"):
        registry.scale_many(
            items=[
                {"slug": "m1-alpha", "concurrency": 10},
                {"slug": "m2-beta", "concurrency": 9999},
            ],
            max_replicas=200,
        )
    assert conn.events == []
    assert not any(sql.strip().upper().startswith("UPDATE") for sql, _ in conn.calls)


def test_scale_many_rejects_duplicate_slugs_and_oversized_batches() -> None:
    conn = _FakeEndpointConn()
    conn.rows["m1-alpha"] = "model-one"
    registry = _registry_with(conn)

    # Two entries for one endpoint would silently last-write-wins.
    with pytest.raises(ValueError, match="duplicate slug"):
        registry.scale_many(
            items=[
                {"slug": "m1-alpha", "concurrency": 1},
                {"slug": "m1-alpha", "concurrency": 2},
            ],
            max_replicas=200,
        )
    with pytest.raises(ValueError, match="per apply"):
        registry.scale_many(
            items=[{"slug": f"m{i}-x", "concurrency": 1} for i in range(51)],
            max_replicas=200,
        )
    assert conn.events == []


def test_scale_many_enforces_the_same_bounds_as_single_row_scale() -> None:
    # The bulk path must not widen the write bounds of a 130-pod fleet: it
    # rejects exactly what scale() rejects and accepts exactly what it accepts.
    conn = _FakeEndpointConn()
    conn.rows["m1-alpha"] = "model-one"
    registry = _registry_with(conn)

    # An unavailable capacity ceiling blocks the bulk path too.
    with pytest.raises(ValueError, match="capacity is unavailable"):
        registry.scale_many(
            items=[{"slug": "m1-alpha", "concurrency": 1}], max_replicas=None
        )
    # The boundary values are accepted on both paths.
    for boundary in (0, 200):
        assert registry.scale_many(
            items=[{"slug": "m1-alpha", "concurrency": boundary}], max_replicas=200
        )["applied"] == [{"slug": "m1-alpha", "concurrency": boundary}]
        assert (
            registry.scale(slug="m1-alpha", concurrency=boundary, max_replicas=200)[
                "concurrency"
            ]
            == boundary
        )


def test_scale_many_allows_latched_breaker_rows_to_be_retargeted() -> None:
    # concurrency is the TARGET the controller honours once Reset clears the
    # latch, not an immediate scale. Refusing the write would strand the operator
    # at a stale target they could only change after unlatching.
    conn = _FakeEndpointConn()
    conn.rows["m1-latched"] = "model-one"
    conn.endpoints["m1-latched"] = {"breaker_open": True}
    registry = _registry_with(conn)

    result = registry.scale_many(
        items=[{"slug": "m1-latched", "concurrency": 12}], max_replicas=200
    )
    assert result["ok"] is True
    assert result["applied"] == [{"slug": "m1-latched", "concurrency": 12}]


def test_update_endpoint_changes_api_and_validates() -> None:
    conn = _FakeEndpointConn()
    conn.rows["m1-alpha"] = "model-one"
    registry = _registry_with(conn)

    result = registry.update(slug="m1-alpha", base_url="https://new.example.com/v1")
    assert result["ok"] is True
    assert any(params[2] == "updated" for params in conn.events)

    with pytest.raises(ValueError, match="base_url"):
        registry.update(slug="m1-alpha", base_url="not-a-url")
    with pytest.raises(ValueError, match="no fields"):
        registry.update(slug="m1-alpha")


def test_update_endpoint_never_logs_token_in_event() -> None:
    conn = _FakeEndpointConn()
    conn.rows["m1-alpha"] = "model-one"
    registry = _registry_with(conn)
    registry.update(slug="m1-alpha", auth_token="ROTATED-SECRET")
    assert all("ROTATED-SECRET" not in str(params) for params in conn.events)


def test_reset_probe_ok_unlatches_and_records_probe_status() -> None:
    from swegen.pipeline.generate_endpoint_controller import ProbeResult

    conn = _FakeEndpointConn()
    conn.rows["m1-alpha"] = "model-one"
    conn.endpoints["m1-alpha"] = {
        "base_url": "https://alpha.example.com",
        "auth_token": "TOP-SECRET-TOKEN",
        "breaker_open": True,
    }
    prober = _FakeProber(ProbeResult(ok=True, status=200, detail="ok"))
    registry = _registry_with(conn, prober=prober)

    result = registry.reset(slug="m1-alpha")

    assert result["ok"] is True
    assert result["unlatched"] is True
    assert result["probe_status"] == 200
    # The unlatch UPDATE ran and cleared the latch, recording last_probe_status.
    reset_calls = [sql for sql, _ in conn.calls if "breaker_open = FALSE" in sql]
    assert reset_calls, "a passing probe should clear the breaker latch"
    assert conn.endpoints["m1-alpha"]["breaker_open"] is False
    assert conn.endpoints["m1-alpha"]["last_probe_status"] == 200
    assert any(params[2] == "reset" for params in conn.events)
    # The prober was invoked with the server-side token; nothing echoes it back.
    assert prober.calls == [("https://alpha.example.com", "model-one", "TOP-SECRET-TOKEN")]
    assert "TOP-SECRET-TOKEN" not in json.dumps(result)


def test_reset_probe_failure_keeps_breaker_latched_and_returns_error_text() -> None:
    from swegen.pipeline.generate_endpoint_controller import ProbeResult

    conn = _FakeEndpointConn()
    conn.rows["m1-alpha"] = "model-one"
    conn.endpoints["m1-alpha"] = {
        "base_url": "https://alpha.example.com",
        "auth_token": "TOP-SECRET-TOKEN",
        "breaker_open": True,
    }
    error_text = (
        "POST https://alpha.example.com/v1/messages -> HTTP 504\n"
        "<html><body>upstream gateway timeout</body></html>"
    )
    prober = _FakeProber(
        ProbeResult(ok=False, status=504, detail="http 504", error_text=error_text)
    )
    registry = _registry_with(conn, prober=prober)

    result = registry.reset(slug="m1-alpha")

    assert result["ok"] is True
    assert result["unlatched"] is False
    assert result["probe_status"] == 504
    # Full, multi-line, copyable error text (with the response body) is returned.
    assert "\n" in result["error_text"]
    assert "HTTP 504" in result["error_text"]
    assert "upstream gateway timeout" in result["error_text"]
    # The breaker was NOT cleared: no unlatch UPDATE ran, breaker stays open.
    assert not [sql for sql, _ in conn.calls if "breaker_open = FALSE" in sql]
    assert conn.endpoints["m1-alpha"]["breaker_open"] is True
    assert conn.endpoints["m1-alpha"]["last_probe_status"] == 504
    # A reset event is still recorded, annotated as a failed probe.
    assert any(params[2] == "reset" for params in conn.events)
    # The token never leaks into the response or the event rows.
    assert "TOP-SECRET-TOKEN" not in json.dumps(result)
    assert all("TOP-SECRET-TOKEN" not in str(params) for params in conn.events)


def test_reset_unknown_slug_raises_not_found() -> None:
    from swegen.dashboard.server import EndpointNotFoundError
    from swegen.pipeline.generate_endpoint_controller import ProbeResult

    prober = _FakeProber(ProbeResult(ok=True, status=200, detail="ok"))
    registry = _registry_with(_FakeEndpointConn(), prober=prober)
    with pytest.raises(EndpointNotFoundError):
        registry.reset(slug="does-not-exist")
    # A non-label-shaped slug is also a 404, not a 500, and never probed.
    with pytest.raises(EndpointNotFoundError):
        registry.reset(slug="Bad Slug!")
    assert prober.calls == []


def test_delete_endpoint_writes_event_before_deleting_row() -> None:
    conn = _FakeEndpointConn()
    conn.rows["m1-alpha"] = "model-one"
    registry = _registry_with(conn)

    result = registry.delete(slug="m1-alpha")
    assert result == {"ok": True, "slug": "m1-alpha"}
    assert "m1-alpha" not in conn.rows
    # The deleted event is recorded before the DELETE statement runs.
    event_index = next(
        i for i, (sql, _) in enumerate(conn.calls) if "GENERATE_ENDPOINT_EVENTS" in sql.upper()
    )
    delete_index = next(
        i for i, (sql, _) in enumerate(conn.calls) if sql.strip().upper().startswith("DELETE")
    )
    assert event_index < delete_index
    assert any(params[2] == "deleted" for params in conn.events)


def test_delete_endpoint_missing_slug_raises_not_found() -> None:
    from swegen.dashboard.server import EndpointNotFoundError

    registry = _registry_with(_FakeEndpointConn())
    with pytest.raises(EndpointNotFoundError):
        registry.delete(slug="ghost-endpoint")


def test_generate_endpoint_routes_are_allowlisted_in_do_post() -> None:
    import inspect

    from swegen.dashboard.server import make_handler

    source = inspect.getsource(make_handler)
    for path in (
        "/api/generate/endpoints",
        "/api/generate/endpoints/scale",
        "/api/generate/endpoints/scale-many",
        "/api/generate/endpoints/update",
        "/api/generate/endpoints/reset",
        "/api/generate/endpoints/delete",
    ):
        assert f'"{path}"' in source
    # Bulk scale is dispatched to the registry's batch method with the same
    # capacity ceiling every other write path uses.
    assert "endpoint_registry.scale_many(" in source
    assert "items=payload.get(\"items\")" in source
    # Its larger body ceiling is scoped to that one route; everything else keeps
    # the original 1 KiB limit.
    assert "MAX_BULK_SCALE_BODY_BYTES" in source
    assert "MAX_CONTROL_BODY_BYTES" in source
    # Endpoint errors map to the documented HTTP status codes.
    assert "EndpointConflictError" in source
    assert "EndpointNotFoundError" in source


def test_parse_range_hours_validates_against_the_allowed_set() -> None:
    from swegen.dashboard.server import RANGE_HOURS_DEFAULT, _parse_range_hours

    assert _parse_range_hours("range_hours=24") == 24
    assert _parse_range_hours("range_hours=72") == 72
    assert _parse_range_hours("range_hours=168") == 168
    # Anything outside the set, junk, or absent falls back to the default.
    assert _parse_range_hours("range_hours=999") == RANGE_HOURS_DEFAULT
    assert _parse_range_hours("range_hours=abc") == RANGE_HOURS_DEFAULT
    assert _parse_range_hours("range_hours=48") == RANGE_HOURS_DEFAULT
    assert _parse_range_hours("") == RANGE_HOURS_DEFAULT
    assert _parse_range_hours("other=5") == RANGE_HOURS_DEFAULT


def test_snapshot_with_range_recomputes_only_the_postgres_section() -> None:
    from swegen.dashboard.server import SnapshotCache

    calls: list[int] = []

    class RangeCollector:
        def collect(self, *, lookback_hours: int = 48) -> dict[str, object]:
            calls.append(lookback_hours)
            return {"stage_model_timeseries": {"lookback_hours": lookback_hours}}

    cache = SnapshotCache()
    cache._snapshot = {
        "postgres": {"cached": True},
        "k3s": {"scaling": {"max_replicas": 512}},
        "buildkit_farm": {"farm": True},
        "sources": {"postgres": {"ok": True, "fetched_at": "t0", "error": None}},
    }
    cache._collectors = {"postgres": RangeCollector()}

    # No range: serves the fully cached snapshot without touching the collector.
    assert cache.snapshot()["postgres"] == {"cached": True}
    assert calls == []

    # A range recomputes only postgres and reuses the cached k3s / farm sections.
    ranged = cache.snapshot(range_hours=72)
    assert calls == [72]
    assert ranged["postgres"]["stage_model_timeseries"]["lookback_hours"] == 72
    assert ranged["k3s"] == {"scaling": {"max_replicas": 512}}
    assert ranged["buildkit_farm"] == {"farm": True}
    assert ranged["sources"]["postgres"]["ok"] is True


def test_do_get_status_threads_validated_range_hours_to_the_cache() -> None:
    from swegen.dashboard.server import (
        GenerateEndpointRegistry,
        K3sBuildSlotController,
        K3sScaler,
        make_handler,
    )

    requested: list[int | None] = []

    class FakeCache:
        def snapshot(self, *, range_hours: int | None = None) -> dict[str, object]:
            requested.append(range_hours)
            return {"range": range_hours}

    handler_cls = make_handler(
        FakeCache(),
        K3sScaler(),
        K3sBuildSlotController(),
        "csrf",
        GenerateEndpointRegistry(),
    )
    sent: list[tuple[int, str, bytes]] = []
    handler = handler_cls.__new__(handler_cls)
    handler._send = lambda status, ctype, body: sent.append((status, ctype, body))

    # A valid range is parsed and threaded through to the cache.
    handler.path = "/api/pipeline/status?range_hours=168"
    handler.do_GET()
    assert requested[-1] == 168
    assert sent[-1][0] == 200
    assert json.loads(sent[-1][2].decode()) == {"range": 168}

    # Junk falls back to the default (24), never reaching the cache unvalidated.
    handler.path = "/api/pipeline/status?range_hours=garbage"
    handler.do_GET()
    assert requested[-1] == 24

    # No query string still resolves to the default range.
    handler.path = "/api/pipeline/status"
    handler.do_GET()
    assert requested[-1] == 24


def test_dashboard_html_has_the_range_dropdown_and_poll_wires_it() -> None:
    from swegen.dashboard.server import HTML

    # A range <select> with exactly the three allowed options sits by the Stages
    # header, labelled "Range: last …".
    assert 'id="range-hours"' in HTML
    assert "Range: last <select" in HTML
    assert '<option value="24">24h</option>' in HTML
    assert '<option value="72">72h</option>' in HTML
    assert '<option value="168">168h</option>' in HTML
    # The selection lives in uiState and is validated against the allowed set.
    assert "rangeHours:24" in HTML
    assert "const RANGE_HOURS_ALLOWED=[24,72,168]" in HTML
    assert "RANGE_HOURS_ALLOWED.includes(value)?value:24" in HTML
    # The poll includes the selected range so all stage charts reflect it, and
    # the selection persists across polls (re-applied to the select on change).
    assert "fetch(`/api/pipeline/status?range_hours=${uiState.rangeHours}`" in HTML
    assert "rangeSelect.addEventListener('change'" in HTML
    # The chart title reflects the selected range instead of a hardcoded 48h.
    assert "`15m outcomes by model · last ${hours}h`" in HTML


def test_dashboard_html_has_generate_endpoints_panel_and_actions() -> None:
    from swegen.dashboard.server import HTML

    # The panel is no longer a top-level <h2> section; it is a titled block that
    # stageCard re-parents into the generate card.
    assert "<h2>Generate model endpoints</h2>" not in HTML
    assert '<div id="endpoints-panel" class="endpoints-panel">' in HTML
    assert (
        '<div class="endpoints-panel-title">Generate model endpoints</div>' in HTML
    )
    # Registration form: url, model, password token, concurrency, register.
    assert 'id="endpoint-form"' in HTML
    assert 'id="endpoint-url"' in HTML
    assert 'id="endpoint-model"' in HTML
    assert 'id="endpoint-token" type="password"' in HTML
    assert 'id="endpoint-concurrency" type="number"' in HTML
    assert 'id="endpoint-register"' in HTML
    assert 'id="endpoints"' in HTML
    # Model and endpoint host are SEPARATE side-by-side columns, not stacked in
    # one cell. Two distinct headers, and renderEndpoints emits two distinct
    # <td> cells (model then host) as the first two columns of each row.
    assert "<th>Model</th><th>Endpoint</th>" in HTML
    assert "<th>Model / endpoint</th>" not in HTML
    assert "setText(modelCell,ep.model_id||ep.slug)" in HTML
    assert "setText(hostCell,ep.host||ep.base_url||'—')" in HTML
    assert "tr.append(modelCell,hostCell,targetCell,outcomeCell,breakerCell,actionsCell)" in HTML
    # The old combined single-cell shape is gone.
    assert "idCell.append(model,host)" not in HTML
    # Empty/unavailable rows span all six columns now.
    assert "td.colSpan=6" in HTML
    # Per-endpoint action buttons wired to their POST routes. Concurrency is no
    # longer a per-row modal button: it is an inline spinner committed by the
    # single bulk Apply (see the endpoint-concurrency tests below).
    assert "submitEndpointScale(ep.slug" not in HTML
    assert "submitEndpointEdit(ep.slug" in HTML
    assert "submitEndpointReset(ep.slug)" in HTML
    assert "submitEndpointDelete(ep.slug" in HTML
    assert "'/api/generate/endpoints/scale-many'" in HTML
    assert "'/api/generate/endpoints/update'" in HTML
    assert "'/api/generate/endpoints/reset'" in HTML
    assert "'/api/generate/endpoints/delete'" in HTML
    # CSRF token is sent like the existing scale submit.
    assert "'X-CSRF-Token':csrfToken" in HTML
    # Reset only offered when the breaker is latched; delete confirms.
    assert "if(ep.breaker_open){const resetBtn" in HTML
    assert "confirm(`Delete endpoint" in HTML
    # The Reset button keeps its label (it now fires a live probe under the hood).
    assert "setText(resetBtn,'Reset')" in HTML


def test_endpoint_rows_have_inline_concurrency_spinners_and_one_bulk_apply() -> None:
    from swegen.dashboard.server import HTML

    # Each row renders a native number input (browser-supplied stepper), bounded
    # by the same cluster ceiling the server enforces.
    assert "function endpointConcInput(ep,maxReplicas,endpoints)" in HTML
    assert "input.type='number'" in HTML
    assert "input.className='endpoint-conc-input'" in HTML
    assert "input.min='0'" in HTML
    assert "input.max=String(maxReplicas)" in HTML
    assert "concCell.append(endpointConcInput(ep,maxReplicas,endpoints))" in HTML
    # Exactly ONE Apply button commits the whole table, not one per row.
    assert 'id="endpoint-apply"' in HTML
    assert HTML.count('id="endpoint-apply"') == 1
    assert "Apply concurrency changes" in HTML
    assert "submitEndpointConcurrencies(endpoints,maxReplicas)" in HTML
    # Drafts are keyed by slug so the 5s poll cannot discard a half-typed edit,
    # and a draft that matches the server value again stops counting as dirty.
    assert "endpointConcDrafts:{}" in HTML
    assert "uiState.endpointConcDrafts[ep.slug]=input.value" in HTML
    assert "if(input.value===String(ep.concurrency??0))delete uiState.endpointConcDrafts[ep.slug]" in HTML
    # Only dirty rows are sent.
    assert "const items=state.dirty.map(ep=>({slug:ep.slug,concurrency:Number(uiState.endpointConcDrafts[ep.slug])}))" in HTML
    # Drafts for endpoints that no longer exist are dropped, so a deleted row
    # cannot leave the Apply button armed forever.
    assert "if(!endpoints.some(ep=>ep.slug===slug))delete uiState.endpointConcDrafts[slug]" in HTML


def test_endpoint_bulk_apply_gates_on_client_side_bounds_and_reports_partials() -> None:
    from swegen.dashboard.server import HTML

    # The button only arms for a dirty, in-range, capacity-available batch -- the
    # same bounds the server enforces, so it never invites a doomed request.
    assert "function endpointApplyState(endpoints,maxReplicas)" in HTML
    assert "!/^\\d+$/.test(String(raw).trim())" in HTML
    assert "value<0||(capacityAvailable&&value>maxReplicas)" in HTML
    assert "enabled:Boolean(dirty.length)&&!invalid.length&&capacityAvailable&&!uiState.endpointBusy" in HTML
    # A missing cluster ceiling blocks the apply outright rather than guessing.
    assert "'Cluster scaling capacity is unavailable; no change was made.'" in HTML
    # A PARTIAL apply names both the applied and the failed rows, so the operator
    # can never read "applied 3" when only 2 landed.
    assert "FAILED ${(body.failed||[]).length}" in HTML
    assert "const failed=(body.failed||[]).map(row=>`${row.slug} (${row.error})`)" in HTML
    # Only server-confirmed rows have their draft cleared; a failed row keeps its
    # pending value so the operator's intent is not silently lost.
    assert "(payload.applied||[]).forEach(row=>delete uiState.endpointConcDrafts[row.slug])" in HTML
    assert "if((payload.failed||[]).length){feedback.className='bad'}" in HTML


def evaluate_endpoint_apply_state(
    endpoints: list[dict[str, object]],
    drafts: dict[str, str],
    max_replicas: object,
    busy: bool = False,
) -> dict[str, object]:
    """Drive the real endpointApplyState/endpointDirtyRows JS over stub rows.

    Exercises the client-side gate itself rather than asserting on its source, so
    a change that silently stops rejecting out-of-range concurrency fails here.
    """

    from swegen.dashboard.server import HTML

    def one_liner(name: str) -> str:
        """Extract a single-line `function <name>(...){...}` definition from HTML."""

        body = HTML.split(f"function {name}", 1)[1].split("\n", 1)[0]
        return f"function {name}{body}"

    program = (
        f"const uiState={{endpointConcDrafts:{json.dumps(drafts)},endpointBusy:{str(busy).lower()}}};\n"
        + one_liner("endpointDirtyRows")
        + "\n"
        + one_liner("endpointApplyState")
        + f"""
const endpoints={json.dumps(endpoints)};
const state=endpointApplyState(endpoints,{json.dumps(max_replicas)});
process.stdout.write(JSON.stringify({{
  dirty:state.dirty.map(e=>e.slug),
  invalid:state.invalid.map(e=>e.slug),
  enabled:state.enabled,
  capacityAvailable:state.capacityAvailable,
}}));
"""
    )
    completed = run(["node", "-e", program], check=True, capture_output=True, text=True)
    return json.loads(completed.stdout)


def test_endpoint_apply_state_arms_only_for_valid_in_range_pending_edits() -> None:
    endpoints = [
        {"slug": "a-one", "concurrency": 130, "model_id": "m-one"},
        {"slug": "b-two", "concurrency": 0, "model_id": "m-two"},
        {"slug": "c-three", "concurrency": 5, "model_id": "m-three"},
    ]

    # No drafts at all: nothing pending, Apply stays disabled.
    idle = evaluate_endpoint_apply_state(endpoints, {}, 200)
    assert idle["dirty"] == [] and idle["enabled"] is False

    # A draft equal to the stored value is NOT dirty (operator typed it back).
    unchanged = evaluate_endpoint_apply_state(endpoints, {"a-one": "130"}, 200)
    assert unchanged["dirty"] == [] and unchanged["enabled"] is False

    # Two genuinely changed rows arm the button; only those two are collected.
    pending = evaluate_endpoint_apply_state(
        endpoints, {"a-one": "160", "c-three": "0"}, 200
    )
    assert pending["dirty"] == ["a-one", "c-three"]
    assert pending["invalid"] == []
    assert pending["enabled"] is True

    # Out of range, negative, and non-integer values each disarm the button.
    for bad in ("201", "-1", "1.5", "abc", ""):
        blocked = evaluate_endpoint_apply_state(endpoints, {"a-one": bad}, 200)
        assert blocked["invalid"] == ["a-one"], bad
        assert blocked["enabled"] is False, bad

    # The upper boundary itself is allowed (matches the server's inclusive bound).
    assert evaluate_endpoint_apply_state(endpoints, {"a-one": "200"}, 200)["enabled"] is True

    # An unavailable cluster ceiling blocks the apply entirely.
    no_capacity = evaluate_endpoint_apply_state(endpoints, {"a-one": "5"}, None)
    assert no_capacity["capacityAvailable"] is False
    assert no_capacity["enabled"] is False

    # A request already in flight disarms it too, so a double-click cannot
    # double-apply a 130-pod fleet change.
    assert (
        evaluate_endpoint_apply_state(endpoints, {"a-one": "9"}, 200, busy=True)["enabled"]
        is False
    )


def render_stage_card_dom(stage: str) -> dict[str, object]:
    """Run the real stageCard() under node against a DOM stub.

    The chart and hourly-yield builders are stubbed (they are covered by their
    own tests); everything stageCard itself does -- the stats block, the
    validate-only queue branch, the generate-only endpoints branch and
    scaleControls -- runs for real, so this observes the actual card structure.
    """

    from swegen.dashboard.server import HTML

    def extract(pattern: str) -> str:
        match = re.search(pattern, HTML, re.S)
        assert match is not None, pattern
        return match.group(0)

    stage_card = extract(
        r"function stageCard\(stage,pg,k,maxReplicas,horizontalChart=false\)\{.*?\n(?=function )"
    )
    stage_stat_lines = extract(r"function stageStatLines\(stage,pg,k\)\{.*?\n(?=function )")
    apply_lines = extract(r"function applyStageStatLines\(card,lines\)\{.*?\n(?=/\*)")
    scale_controls = extract(r"function scaleControls\(stage,desired,maxReplicas\)\{.*?\n(?=function )")
    validate_queue_line = extract(r"function validateQueueLine\(label,queue\)\{.*?\n(?=function )")
    format_pod_phases = extract(r"const formatPodPhases=.*?\n")
    format_coverage = extract(r"function formatInstanceCoverage\(count,total\)\{.*?\n(?=function )")
    set_text = extract(r"function setText\(node,value\)\{.*?\n")

    program = (
        """
const TAG_RE=/<(\\w+)(?:[^>]*class="([^"]*)")?[^>]*>/g;
class El{
  constructor(tag){this.tag=tag;this.children=[];this.className='';this.textContent='';
    this.dataset={};this.attrs={};this.parent=null;this.type='';this.value='';
    this.min='';this.max='';this.step='';this.disabled=false;}
  append(...kids){kids.forEach(kid=>{if(kid.parent){const siblings=kid.parent.children;
    const at=siblings.indexOf(kid);if(at>=0)siblings.splice(at,1)}
    kid.parent=this;this.children.push(kid)})}
  after(node){const siblings=this.parent.children;
    siblings.splice(siblings.indexOf(this)+1,0,node);node.parent=this.parent}
  setAttribute(key,value){this.attrs[key]=value}
  addEventListener(){}
  set innerHTML(html){this.children=[];TAG_RE.lastIndex=0;let match;
    while((match=TAG_RE.exec(html))!==null){const node=new El(match[1]);
      node.className=match[2]||'';node.parent=this;this.children.push(node)}}
  matches(sel){return sel.startsWith('.')?this.className.split(' ').includes(sel.slice(1))
    :this.tag===sel}
  querySelectorAll(sel){const found=[];(function walk(node){node.children.forEach(kid=>{
    if(kid.matches(sel))found.push(kid);walk(kid)})})(this);return found}
  querySelector(sel){return this.querySelectorAll(sel)[0]||null}}
globalThis.document={createElement:tag=>new El(tag)};
const endpointsPanel=new El('div');endpointsPanel.className='endpoints-panel';
endpointsPanel.dataset.sentinel='the-one-and-only';
const el=id=>id==='endpoints-panel'?endpointsPanel:null;
const stageNames={generate:'SWEgen',validate:'NOP / Oracle',repair:'Repair',
  reward:'Reward hack',push:'SWR push'};
const uiState={scaleDrafts:{},scaling:false};
const divergingModelTimeSeries=()=>{const n=new El('div');n.className='stage-chart-wrap';return n};
const stageHourlyYield=()=>{const n=new El('div');n.className='stage-yield';return n};
"""
        + set_text
        + format_pod_phases
        + format_coverage
        + validate_queue_line
        + scale_controls
        + stage_stat_lines
        + apply_lines
        + stage_card
        + """
const pg={queues:{stages:{},validate_repaired:{},validate_new:{}},activity:{stages:{}},
  throughput:{windows:{'300':{}},lifetime_processed:{}},instance_coverage:{},
  stage_model_timeseries:{stages:{}},hourly_yield:{stages:{}}};
const k={stages:{"""
        + f"{stage}:{{desired:12,pod_phases:{{Running:12}}}}"
        + """}};
const card=stageCard('"""
        + stage
        + """',pg,k,64,true);
const describe=node=>({tag:node.tag,className:node.className,
  sentinel:node.dataset.sentinel||null,children:node.children.map(describe)});
console.log(JSON.stringify({card:describe(card),
  panelReparented:endpointsPanel.parent===card,
  panelStillDetached:endpointsPanel.parent===null,
  directChildClasses:card.children.map(child=>child.className)}));
"""
    )
    completed = run(["node", "-e", program], check=True, capture_output=True, text=True)
    return json.loads(completed.stdout)


def _class_names(tree: dict) -> list[str]:
    names = [tree["className"]]
    for child in tree["children"]:
        names.extend(_class_names(child))
    return names


def test_endpoints_panel_is_reparented_into_the_generate_stage_card() -> None:
    rendered = render_stage_card_dom("generate")

    # The panel node from the document (not a copy) becomes a child of the card,
    # so every id, listener and the live #endpoints tbody keep working.
    assert rendered["panelReparented"] is True
    assert "endpoints-panel" in _class_names(rendered["card"])
    sentinels = [
        node
        for node in _flatten(rendered["card"])
        if node["sentinel"] == "the-one-and-only"
    ]
    assert len(sentinels) == 1, "panel must be moved, never duplicated"
    # It is a DIRECT child of the card (so the grid-column:1/-1 full-width rule
    # applies) and sits last, after stats/chart/yield.
    assert rendered["directChildClasses"] == [
        "stage-stats",
        "stage-chart-wrap",
        "stage-yield",
        "endpoints-panel",
    ]


def _flatten(tree: dict) -> list[dict]:
    nodes = [tree]
    for child in tree["children"]:
        nodes.extend(_flatten(child))
    return nodes


def test_generate_card_drops_the_stage_wide_scale_spinner() -> None:
    rendered = render_stage_card_dom("generate")

    # Generate is no longer one pool at one concurrency: it fans out into one
    # deployment per registered endpoint, each scaled from the endpoints table
    # in this same card. A single stage-wide replica spinner would lie about it.
    assert "scale-controls" not in _class_names(rendered["card"])
    assert "scale-limit" not in _class_names(rendered["card"])


@pytest.mark.parametrize("stage", ["validate", "repair", "reward", "push"])
def test_single_pool_stage_cards_keep_their_scale_spinner(stage: str) -> None:
    rendered = render_stage_card_dom(stage)

    # These stages are still one Deployment each, and this spinner is the only
    # way to scale them from the UI, so it must survive.
    classes = _class_names(rendered["card"])
    assert "scale-controls" in classes
    assert "scale-limit" in classes
    # And they never receive the generate-only endpoints panel.
    assert "endpoints-panel" not in classes
    assert rendered["panelStillDetached"] is True


def test_stage_card_branches_on_generate_for_the_endpoints_panel() -> None:
    from swegen.dashboard.server import HTML

    # Generate takes the endpoints branch INSTEAD of the scale spinner; every
    # other stage still appends scaleControls.
    assert (
        "if(stage==='generate'){card._endpointsPanel=el('endpoints-panel')}"
        "else{stats.append(scaleControls(stage,lines.desired,maxReplicas))}" in HTML
    )
    # The panel is appended to the card itself, not into the stats column.
    assert "if(card._endpointsPanel)card.append(card._endpointsPanel)" in HTML
    # scaleControls itself, and the stage-wide scale route it posts to, survive.
    assert "function scaleControls(stage,desired,maxReplicas)" in HTML
    assert "fetch('/api/pipeline/scale'" in HTML
    # The panel is full-width inside the 3-column horizontal card.
    assert ".endpoints-panel{grid-column:1/-1" in HTML


def test_reset_probe_ui_reports_both_unlatched_outcomes() -> None:
    from swegen.dashboard.server import HTML

    # The reset submit branches on the probe outcome the API returns.
    assert "body.unlatched?" in HTML
    assert "breaker cleared." in HTML
    assert "breaker kept latched." in HTML
    # A failed probe renders the full error into a readonly, copyable box, and a
    # success clears any previous box.
    assert "renderResetError(slug,payload.error_text" in HTML
    assert "clearResetError()" in HTML
    assert 'id="endpoint-reset-error"' in HTML
    assert "ta.readOnly=true" in HTML
    assert "navigator.clipboard.writeText" in HTML
    # The token input is never rendered back into the table.
    assert "renderEndpoints(pg.generate_endpoints,k.generate_endpoint_pods||{},maxReplicas)" in HTML
    assert "ep.auth_token" not in HTML


def test_dashboard_stage_cards_stack_vertically() -> None:
    from swegen.dashboard.server import HTML

    # The stage flow container is a single full-width column, not the old
    # 4-across grid, so each taller diverging-chart card has room.
    assert ".pipeline-flow{display:grid;grid-template-columns:1fr;" in HTML
    assert (
        "minmax(235px,1fr) minmax(520px,2fr) minmax(235px,1fr) minmax(235px,1fr)" not in HTML
    )
    # The diverging chart internals are untouched.
    assert "chart.className='chart chart-diverging'" in HTML
    assert "chartRow.append(chart,yieldView);card.append(stats,chartRow)" in HTML


def _export_handler(monkeypatch, records_by_registry: dict[str, list[dict[str, object]]]):
    """A handler instance whose wfile/headers are captured instead of socketed."""

    from swegen.dashboard import server as server_module
    from swegen.dashboard.server import (
        GenerateEndpointRegistry,
        K3sBuildSlotController,
        K3sScaler,
        make_handler,
    )

    def fake_export(registry: str):
        yield from records_by_registry.get(registry, [])

    monkeypatch.setattr(server_module, "iter_pushed_image_export", fake_export)

    class FakeCache:
        def snapshot(self, *, range_hours: int | None = None) -> dict[str, object]:
            return {}

    handler_cls = make_handler(
        FakeCache(),
        K3sScaler(),
        K3sBuildSlotController(),
        "csrf",
        GenerateEndpointRegistry(),
    )
    handler = handler_cls.__new__(handler_cls)
    captured: dict[str, object] = {"status": None, "headers": [], "body": bytearray()}

    class _Wfile:
        def write(self, chunk: bytes) -> None:
            captured["body"].extend(chunk)

    handler.send_response = lambda status: captured.__setitem__("status", status)
    handler.send_header = lambda key, value: captured["headers"].append((key, value))
    handler.end_headers = lambda: None
    handler.wfile = _Wfile()
    handler._send = lambda status, ctype, body: captured.update(
        {"status": status, "headers": [("Content-Type", ctype)], "body": bytearray(body)}
    )
    return handler, captured


_EXPORT_RECORDS = [
    {
        "instance_id": "01mf02__jaq-100",
        "repo": "01mf02/jaq",
        "pr": 100,
        "registry": "platform",
        "registry_path": "swr-data-platform.example.com/swegen/generated:01mf02__jaq-100",
        "created_at": "2026-07-31T20:29:27+00:00",
        "pushed_at": "2026-08-02T09:35:42+00:00",
    },
    {
        "instance_id": "zed__zed-42",
        "repo": "zed/zed",
        "pr": 42,
        "registry": "platform",
        "registry_path": "swr-data-platform.example.com/swegen/generated:zed__zed-42",
        "created_at": "2026-08-01T01:02:03+00:00",
        "pushed_at": "2026-08-03T04:05:06+00:00",
    },
]


def test_pushed_image_export_emits_one_json_object_per_line(monkeypatch) -> None:
    handler, captured = _export_handler(monkeypatch, {"platform": _EXPORT_RECORDS})
    handler.path = "/api/pushed-images/platform.jsonl"
    handler.do_GET()

    assert captured["status"] == 200
    body = bytes(captured["body"]).decode()
    # Newline-delimited: one parseable object per line, no trailing blank object.
    lines = body.splitlines()
    assert len(lines) == 2
    assert body.endswith("\n")
    parsed = [json.loads(line) for line in lines]
    assert [row["instance_id"] for row in parsed] == ["01mf02__jaq-100", "zed__zed-42"]
    # Every promised field rides each line.
    for row in parsed:
        assert set(row) >= {
            "instance_id",
            "repo",
            "pr",
            "registry",
            "registry_path",
            "created_at",
            "pushed_at",
        }
        assert "/" in row["repo"]
        assert row["registry"] == "platform"


def test_pushed_image_export_sets_ndjson_and_a_dated_attachment_filename(
    monkeypatch,
) -> None:
    handler, captured = _export_handler(monkeypatch, {"trajectory": _EXPORT_RECORDS})
    handler.path = "/api/pushed-images/trajectory.jsonl"
    handler.do_GET()

    headers = dict(captured["headers"])
    assert headers["Content-Type"] == "application/x-ndjson; charset=utf-8"
    disposition = headers["Content-Disposition"]
    assert disposition.startswith('attachment; filename="pushed-images-trajectory-')
    assert re.search(r"-\d{8}\.jsonl\"$", disposition)
    assert headers["Cache-Control"] == "no-store"
    # No Content-Length: the body is streamed, so its size is unknown up front.
    assert "Content-Length" not in headers


def test_pushed_image_export_of_an_empty_set_is_an_empty_body_not_an_error(
    monkeypatch,
) -> None:
    handler, captured = _export_handler(monkeypatch, {})
    handler.path = "/api/pushed-images/platform.jsonl"
    handler.do_GET()

    assert captured["status"] == 200
    assert bytes(captured["body"]) == b""


def test_pushed_image_export_streams_instead_of_buffering_the_whole_manifest(
    monkeypatch,
) -> None:
    # The 11k-row platform manifest must never be joined into one string: assert
    # the body reaches the socket incrementally, one write per record.
    handler, captured = _export_handler(
        monkeypatch, {"platform": [dict(_EXPORT_RECORDS[0]) for _ in range(5)]}
    )
    writes: list[bytes] = []
    handler.wfile.write = writes.append
    handler.path = "/api/pushed-images/platform.jsonl"
    handler.do_GET()

    assert len(writes) == 5
    assert all(chunk.endswith(b"\n") for chunk in writes)


def test_pushed_image_export_reports_a_db_failure_before_sending_a_200(
    monkeypatch,
) -> None:
    from swegen.dashboard import server as server_module

    def exploding_export(registry: str):
        raise RuntimeError("connection refused")
        yield  # pragma: no cover - generator marker

    handler, captured = _export_handler(monkeypatch, {})
    monkeypatch.setattr(server_module, "iter_pushed_image_export", exploding_export)
    handler.path = "/api/pushed-images/platform.jsonl"
    handler.do_GET()

    # A truncated 200 would be indistinguishable from a short manifest, so the
    # failure has to surface as a status code instead.
    assert captured["status"] == 502
    assert "connection refused" in json.loads(bytes(captured["body"]).decode())["error"]


def test_unknown_pushed_image_registry_route_is_a_404(monkeypatch) -> None:
    handler, captured = _export_handler(monkeypatch, {"platform": _EXPORT_RECORDS})
    handler.path = "/api/pushed-images/secrets.jsonl"
    handler.do_GET()

    assert captured["status"] == 404


def test_pushed_image_export_routes_are_an_exact_match_table() -> None:
    from swegen.dashboard.server import PUSHED_IMAGE_EXPORT_ROUTES

    assert PUSHED_IMAGE_EXPORT_ROUTES == {
        "/api/pushed-images/platform.jsonl": "platform",
        "/api/pushed-images/trajectory.jsonl": "trajectory",
    }


def test_swr_push_cards_carry_jsonl_download_buttons() -> None:
    from swegen.dashboard.server import HTML

    # farmCard takes an OPTIONAL action element, so the cards that do not pass
    # one keep their previous markup.
    assert "function farmCard(summary,label,value,detail,action)" in HTML
    assert "if(action)card.append(action)" in HTML
    assert "function downloadButton(label,href)" in HTML
    # Both push-registry cards get a download button pointing at their endpoint;
    # the two neighbouring cards in the same grid do not.
    assert (
        "farmCard(summary,'Pushed to -platform',compactChartCount(sync.platform_count||0),"
        "`${sync.platform_count||0} distinct images on data-platform`,"
        "downloadButton('Download JSONL','/api/pushed-images/platform.jsonl'))" in HTML
    )
    assert (
        "farmCard(summary,'Pushed to -trajectory',"
        "compactChartCount(sync.trajectory_count||0),"
        "`${sync.trajectory_count||0} distinct images on data-trajectory`,"
        "downloadButton('Download JSONL','/api/pushed-images/trajectory.jsonl'))" in HTML
    )
    assert "farmCard(summary,'Platform only (missing -trajectory)'" in HTML
    assert HTML.count("downloadButton('Download JSONL'") == 2
    # Styled with the dashboard's existing button palette.
    assert ".card-download{" in HTML
    assert "background:#174b78" in HTML


def evaluate_push_card_render() -> dict[str, object]:
    """Run renderSwrPushSync's card loop under node against a DOM stub."""

    from swegen.dashboard.server import HTML

    farm_card = re.search(r"function farmCard\(summary.*?\n", HTML).group(0)
    download = re.search(r"function downloadButton\(label,href\).*?\n", HTML).group(0)
    program = (
        """
class El{constructor(tag){this.tag=tag;this.children=[];this.className='';
  this.textContent='';this.attrs={};this.href='';}
 append(...kids){this.children.push(...kids)}
 setAttribute(k,v){this.attrs[k]=v}}
globalThis.document={createElement:t=>new El(t)};
globalThis.setText=(n,v)=>{n.textContent=String(v)};
const compactChartCount=v=>String(v);
"""
        + farm_card
        + download
        + """
const summary=new El('div');const sync={platform_count:11615,trajectory_count:6443,
  platform_only:5172,trajectory_only:0};
"""
        + re.search(
            r"farmCard\(summary,'Pushed to -platform'.*?"
            r"farmCard\(summary,'Trajectory only \(missing -platform\)'[^;]*;",
            HTML,
            re.S,
        ).group(0)
        + """
const cards=summary.children.map(card=>{
  const link=[];(function walk(n){if(n.tag==='a')link.push({text:n.textContent,
    href:n.href,download:n.attrs.download!==undefined});
    (n.children||[]).forEach(walk)})(card);
  return {label:card.children[0].textContent,value:card.children[1].textContent,
    links:link}});
console.log(JSON.stringify(cards));
"""
    )
    result = run(["node", "-e", program], check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def test_push_cards_render_download_links_without_disturbing_the_other_cards() -> None:
    cards = evaluate_push_card_render()

    # All four cards still render, in order, with their counts intact.
    assert [card["label"] for card in cards] == [
        "Pushed to -platform",
        "Pushed to -trajectory",
        "Platform only (missing -trajectory)",
        "Trajectory only (missing -platform)",
    ]
    assert [card["value"] for card in cards] == ["11615", "6443", "5172", "0"]
    # Only the two registry cards gain a download link, each to its own endpoint
    # and marked `download` so the browser saves rather than navigates.
    assert cards[0]["links"] == [
        {
            "text": "Download JSONL",
            "href": "/api/pushed-images/platform.jsonl",
            "download": True,
        }
    ]
    assert cards[1]["links"] == [
        {
            "text": "Download JSONL",
            "href": "/api/pushed-images/trajectory.jsonl",
            "download": True,
        }
    ]
    assert cards[2]["links"] == []
    assert cards[3]["links"] == []
