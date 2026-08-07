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
    assert "uiState.chartScroll[stage]=chartScrollSnapshot(chart)" in HTML
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
    assert "bucket.addEventListener('mouseenter'" in HTML
    assert "bucket.addEventListener('mousemove',positionChartTooltip)" in HTML
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
    assert "chart.className='chart'" in HTML
    assert "yAxis.className='chart-y-axis'" in HTML
    assert "xTick.className='x-tick'" in HTML
    assert "compactChartTimestamp(row.bucket)" in HTML
    assert ".x-tick::before{" in HTML


def test_chart_timestamp_density_responds_to_available_width() -> None:
    from swegen.dashboard.server import HTML

    narrow, _, _ = evaluate_chart_axis_helpers(24, 240, 950)
    wide, compact_count, _ = evaluate_chart_axis_helpers(24, 960, 1_250_000)

    assert narrow == 6
    assert wide == 2
    assert compact_count == "1.3m"
    assert "new ResizeObserver(()=>updateChartTicks(chart))" in HTML
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


def test_dashboard_places_resources_and_storage_immediately_before_recent_tasks() -> None:
    from swegen.dashboard.server import HTML

    stages = HTML.index("<h2>Stages</h2>")
    hourly_yield = HTML.index("<h2>Hourly yield</h2>")
    resources = HTML.index("<h2>Cluster resources</h2>")
    storage = HTML.index("<h2>Harbor task storage</h2>")
    buildkit_farm = HTML.index("<h2>Remote BuildKit farm</h2>")
    recent_tasks = HTML.index("<h2>Recent tasks</h2>")

    assert stages < hourly_yield < resources < storage < buildkit_farm < recent_tasks
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
    assert "formatDiskIo(node.disk_io)" in HTML
    assert "30-second cAdvisor rate sample" in HTML
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
    assert "lifetime processed <b>${lifetime}</b>" in HTML


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
    assert "unique iids <b>${formatInstanceCoverage(unique,universe)}</b>" in HTML


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


def render_diverging_model_chart(stage_data: object, stage: str = "generate") -> dict[str, object]:
    from swegen.dashboard.server import HTML

    consts = "".join(
        re.search(re.escape(prefix) + r".*?;\n", HTML).group(0)
        for prefix in (
            "const MODEL_PALETTE=",
            "const modelColor=",
            "const compactChartCount=",
            "const compactChartTimestamp=",
        )
    )
    renderer = re.search(
        r"function divergingModelTimeSeries\(stageData,stage\)\{.*?\n(?=function stageTimeSeries)",
        HTML,
        re.S,
    )
    assert renderer is not None
    program = (
        _DOM_STUB
        + consts
        + renderer.group(0)
        + f"const wrap=divergingModelTimeSeries({json.dumps(stage_data)},{json.dumps(stage)});"
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


def test_all_stages_route_to_the_diverging_per_model_chart() -> None:
    from swegen.dashboard.server import HTML

    # Every stage card now renders the shared per-model diverging chart, reading
    # its stage's slice of the unified stage_model_timeseries structure.
    assert (
        "divergingModelTimeSeries(pg.stage_model_timeseries?.stages?.[stage],stage)"
        in HTML
    )
    assert "function divergingModelTimeSeries(stageData,stage)" in HTML
    # The generate-only special-case ternary is gone.
    assert "stage==='generate'?generateModelTimeSeries" not in HTML
    assert "function generateModelTimeSeries" not in HTML
    # A central baseline (bottom border on the up slot) and diverging slots exist.
    assert ".bar-slot-up{align-items:flex-end;border-bottom:1px solid var(--line)}" in HTML
    assert ".bar-slot-down{align-items:flex-start}" in HTML
    # The y-axis is symmetric around zero (max, 0, -max).
    assert "[max,0,-max].forEach" in HTML
    assert "success up · failed down" in HTML
    # Per-stage scroll keys are distinct (stage-derived, not a hardcoded literal).
    assert "const scrollKey=`${stage}-model`" in HTML
    assert "chart.dataset.stage=scrollKey" in HTML
    assert "restoreChartScroll(chart,scrollKey)" in HTML


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
    rendered = render_diverging_model_chart(stage_data, "validate")
    assert rendered["empty"] is False
    assert [seg["bg"] for seg in rendered["up"]] == ["#3987e5", "#199e70"]
    assert {seg["op"] for seg in rendered["up"]} == {"1"}
    assert [seg["bg"] for seg in rendered["down"]] == ["#3987e5", "#199e70"]
    assert {seg["op"] for seg in rendered["down"]} == {"0.55"}
    assert rendered["legend"] == ["#3987e5", "#199e70"]
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
    rendered = render_diverging_model_chart(model, "generate")
    assert rendered["empty"] is False
    # Two model colours appear above the axis at full opacity (success).
    assert [seg["bg"] for seg in rendered["up"]] == ["#3987e5", "#199e70"]
    assert {seg["op"] for seg in rendered["up"]} == {"1"}
    # The same two colours appear below the axis, faded, for failures.
    assert [seg["bg"] for seg in rendered["down"]] == ["#3987e5", "#199e70"]
    assert {seg["op"] for seg in rendered["down"]} == {"0.55"}
    # The legend maps each colour to a model_id.
    assert rendered["legend"] == ["#3987e5", "#199e70"]
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


def test_top_stage_cards_separate_fresh_activity_from_queue_leases() -> None:
    from swegen.dashboard.server import HTML

    assert "pg.activity?.stages?.[stage]" in HTML
    assert "active <b>${a.fresh||0}</b>" in HTML
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

    assert '<div class="big">${w.pod_phases?.Running||0} Running</div>' in HTML
    assert "formatPodPhases(w.pod_phases,w.evicted||0)" in HTML
    assert "desired <b>${w.desired||0}</b>" in HTML
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

    assert "flow.append(stageCard('generate'" in HTML
    assert "validationLoop,stageCard('reward'" in HTML
    assert "stageCard('push',pg,k,maxReplicas))" in HTML


def test_nested_validation_stage_cards_preserve_metrics_and_controls() -> None:
    from swegen.dashboard.server import HTML

    assert "function stageCard(stage,pg,k,maxReplicas,horizontalChart=false)" in HTML
    assert "stats.append(scaleControls(stage,w.desired||0,maxReplicas))" in HTML
    assert "5m success <b>${t.succeeded||0}</b>" in HTML
    assert "lifetime processed <b>${lifetime}</b>" in HTML


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

    assert "card.append(stats,chart)" in HTML
    # Every stage routes to the shared diverging per-model chart, reading its
    # slice of the unified stage_model_timeseries structure.
    assert (
        "divergingModelTimeSeries(pg.stage_model_timeseries?.stages?.[stage],stage)" in HTML
    )
    assert "stageCard('validate',pg,k,maxReplicas,true)" in HTML
    assert "stageCard('repair',pg,k,maxReplicas,true)" in HTML
    assert ".stage-card-horizontal{display:grid;grid-template-columns:240px minmax(0,1fr)" in HTML
    assert (
        ".stage-card:not(.stage-card-horizontal) .stage-chart-wrap{flex:1;margin-top:10px" in HTML
    )
    assert ".stage-card-horizontal .stage-chart-wrap{border-left:1px solid var(--line)" in HTML
    assert ".stage-card{display:flex;flex-direction:column;padding:11px}" in HTML
    assert ".validation-loop{" in HTML and "align-content:stretch" in HTML
    assert "15m outcomes · last 48h" in HTML


def test_stage_charts_grow_without_centering_margins() -> None:
    from swegen.dashboard.server import HTML

    assert ".stage-chart-wrap{min-width:0;min-height:0;display:flex;flex-direction:column}" in HTML
    assert ".stage-card:not(.stage-card-horizontal) .stage-chart-wrap{flex:1" in HTML
    assert ".chart-frame{min-width:0;min-height:112px;flex:1" in HTML
    assert ".chart{min-height:112px;min-width:0" in HTML
    assert "bar.style.height=`${Math.max(2,total/max*100)}%`" in HTML
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
        if normalized.startswith("INSERT INTO GENERATE_ENDPOINTS"):
            self.rows[params[0]] = params[2]
            return _FakeResult(rowcount=1)
        if normalized.startswith("INSERT INTO GENERATE_ENDPOINT_EVENTS"):
            self.events.append(params)
            return _FakeResult(rowcount=1)
        if normalized.startswith("UPDATE"):
            return _FakeResult(rowcount=1 if params[-1] in self.rows else 0)
        if normalized.startswith("DELETE"):
            self.rows.pop(params[0], None)
            return _FakeResult(rowcount=1)
        return _FakeResult()


def _registry_with(conn: _FakeEndpointConn):
    from swegen.dashboard.server import GenerateEndpointRegistry

    return GenerateEndpointRegistry(connect=lambda: conn)


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


def test_reset_endpoint_clears_breaker_latch() -> None:
    conn = _FakeEndpointConn()
    conn.rows["m1-alpha"] = "model-one"
    registry = _registry_with(conn)

    result = registry.reset(slug="m1-alpha")
    assert result == {"ok": True, "slug": "m1-alpha"}
    reset_calls = [sql for sql, _ in conn.calls if "breaker_open = FALSE" in sql]
    assert reset_calls, "reset should clear the breaker latch"
    assert any(params[2] == "reset" for params in conn.events)


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
        "/api/generate/endpoints/update",
        "/api/generate/endpoints/reset",
        "/api/generate/endpoints/delete",
    ):
        assert f'"{path}"' in source
    # Endpoint errors map to the documented HTTP status codes.
    assert "EndpointConflictError" in source
    assert "EndpointNotFoundError" in source


def test_dashboard_html_has_generate_endpoints_panel_and_actions() -> None:
    from swegen.dashboard.server import HTML

    assert "<h2>Generate model endpoints</h2>" in HTML
    # Registration form: url, model, password token, concurrency, register.
    assert 'id="endpoint-form"' in HTML
    assert 'id="endpoint-url"' in HTML
    assert 'id="endpoint-model"' in HTML
    assert 'id="endpoint-token" type="password"' in HTML
    assert 'id="endpoint-concurrency" type="number"' in HTML
    assert 'id="endpoint-register"' in HTML
    assert 'id="endpoints"' in HTML
    # Per-endpoint action buttons wired to their POST routes.
    assert "submitEndpointScale(ep.slug" in HTML
    assert "submitEndpointEdit(ep.slug" in HTML
    assert "submitEndpointReset(ep.slug)" in HTML
    assert "submitEndpointDelete(ep.slug" in HTML
    assert "'/api/generate/endpoints/scale'" in HTML
    assert "'/api/generate/endpoints/update'" in HTML
    assert "'/api/generate/endpoints/reset'" in HTML
    assert "'/api/generate/endpoints/delete'" in HTML
    # CSRF token is sent like the existing scale submit.
    assert "'X-CSRF-Token':csrfToken" in HTML
    # Reset only offered when the breaker is latched; delete confirms.
    assert "if(ep.breaker_open){const resetBtn" in HTML
    assert "confirm(`Delete endpoint" in HTML
    # The token input is never rendered back into the table.
    assert "renderEndpoints(pg.generate_endpoints,k.generate_endpoint_pods||{})" in HTML
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
    assert "card.append(stats,chart)" in HTML
