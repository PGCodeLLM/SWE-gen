from __future__ import annotations

import re
from subprocess import CompletedProcess, run

import pytest


def evaluate_chart_scroll_target(saved: int | None, max_scroll: int) -> int:
    from swegen.dashboard.server import HTML

    match = re.search(
        r"function chartScrollTarget\(saved,maxScroll\)\{[^}]+\}",
        HTML,
    )
    assert match is not None
    saved_javascript = "undefined" if saved is None else str(saved)
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


def evaluate_chart_tooltip() -> dict[str, object]:
    from swegen.dashboard.server import HTML

    functions = "function positionChartTooltip" + HTML.split(
        "function positionChartTooltip", 1
    )[1].split("function scaleControls", 1)[0]
    script = """
const tooltip={hidden:true,textContent:'',style:{},offsetWidth:100,offsetHeight:30};
const el=id=>tooltip;
function setText(node,value){node.textContent=value==null?'—':String(value)}
const window={innerWidth:500,innerHeight:300};
const event={type:'mouseenter',clientX:40,clientY:50,currentTarget:{getBoundingClientRect(){return {left:0,top:0,width:10}}}};
showChartTooltip(event,'Jul 31 · success 3 · failed 1');
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

    assert evaluate_chart_scroll_target(137, 417) == 137
    assert evaluate_chart_scroll_target(0, 417) == 0
    assert "hasOwnProperty.call(uiState.chartScroll,stage)" in HTML
    assert "uiState.chartScroll[stage]=chart.scrollLeft" in HTML


def test_chart_tooltip_appears_immediately_and_hides_on_leave() -> None:
    from swegen.dashboard.server import HTML

    result = evaluate_chart_tooltip()

    assert result == {
        "shown": {
            "hidden": False,
            "text": "Jul 31 · success 3 · failed 1",
            "left": "52px",
            "top": "62px",
        },
        "hiddenAfterLeave": True,
    }
    assert "bucket.title=" not in HTML
    assert "bucket.addEventListener('mouseenter'" in HTML
    assert "bucket.addEventListener('mousemove',positionChartTooltip)" in HTML
    assert 'id="chart-tooltip"' in HTML


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
        "Sampled worker queue",
        "Active builds",
        "SWEgen remote pending",
    ):
        assert title in HTML
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
    assert "formatBuildSlots(node.build_slots)" in HTML
    assert "${slots.used}/${slots.total} used" in HTML
    assert "waiters ${slots.waiters??'unknown'}" in HTML
    assert "Wrapper does not persist waiter depth; unknown is explicit." in HTML
    assert "td.colSpan=5" in HTML


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

    assert "card.append(stats,stageTimeSeries(stage,pg.stage_time_series))" in HTML
    assert "stageCard('validate',pg,k,maxReplicas,true)" in HTML
    assert "stageCard('repair',pg,k,maxReplicas,true)" in HTML
    assert ".stage-card-horizontal{display:grid;grid-template-columns:240px minmax(0,1fr)" in HTML
    assert ".stage-card:not(.stage-card-horizontal) .stage-chart-wrap{flex:1;margin-top:10px" in HTML
    assert ".stage-card-horizontal .stage-chart-wrap{border-left:1px solid var(--line)" in HTML
    assert ".stage-card{display:flex;flex-direction:column;padding:11px}" in HTML
    assert ".validation-loop{" in HTML and "align-content:stretch" in HTML
    assert "15m outcomes · last 6h" in HTML


def test_stage_charts_grow_without_centering_margins() -> None:
    from swegen.dashboard.server import HTML

    assert ".stage-chart-wrap{min-width:0;min-height:0;display:flex;flex-direction:column}" in HTML
    assert ".stage-card:not(.stage-card-horizontal) .stage-chart-wrap{flex:1" in HTML
    assert ".chart{min-height:96px;flex:1" in HTML
    assert "bar.style.height=`${Math.max(2,total/max*100)}%`" in HTML
    assert "justify-content:center;padding:11px" not in HTML


def test_horizontal_validation_stats_are_fixed_width_and_left_aligned() -> None:
    from swegen.dashboard.server import HTML

    assert ".stage-card-horizontal .stage-stats{width:240px;text-align:left" in HTML
    assert "justify-self:start;align-self:start" in HTML


def test_generate_total_is_apportioned_across_main_and_overflow() -> None:
    from swegen.dashboard.server import K3sScaler

    assert K3sScaler.plan("generate", 96, max_replicas=768) == [
        ("swegen-generate", 92),
        ("swegen-generate-overflow", 4),
    ]
    assert K3sScaler.plan("generate", 80, max_replicas=768) == [
        ("swegen-generate-overflow", 0),
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

    assert applied == [
        {"deployment": "swegen-generate", "replicas": 92},
        {"deployment": "swegen-generate-overflow", "replicas": 4},
    ]
    assert commands == [
        [
            "kubectl",
            "--request-timeout=10s",
            "-n",
            "swegen-pipeline",
            "scale",
            "deployment/swegen-generate",
            "--replicas=92",
        ],
        [
            "kubectl",
            "--request-timeout=10s",
            "-n",
            "swegen-pipeline",
            "scale",
            "deployment/swegen-generate-overflow",
            "--replicas=4",
        ],
    ]


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
