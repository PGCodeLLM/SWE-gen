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


def test_top_stage_cards_separate_fresh_activity_from_queue_leases() -> None:
    from swegen.dashboard.server import HTML

    assert "pg.activity?.stages?.[stage]" in HTML
    assert "active <b>${a.fresh||0}</b>" in HTML
    assert "leased ${q.in_flight||0}" in HTML
    assert "stale ${stale}" in HTML


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

    assert "card.append(stats,stageTimeSeries(stage,pg.stage_time_series))" in HTML
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
        if "get" in command and "deployment/swegen-generate" in command:
            return CompletedProcess(
                command,
                0,
                stdout=json.dumps(
                    {
                        "spec": {
                            "template": {
                                "spec": {
                                    "containers": [
                                        {
                                            "name": "worker",
                                            "image": "swegen-worker:working",
                                            "envFrom": [
                                                {
                                                    "secretRef": {
                                                        "name": "swegen-model-credentials-sol-direct"
                                                    }
                                                }
                                            ],
                                        }
                                    ]
                                }
                            }
                        }
                    }
                ),
                stderr="",
            )
        if "get" in command and "deployment/swegen-generate-overflow" in command:
            return CompletedProcess(
                command,
                0,
                stdout=json.dumps(
                    {
                        "spec": {
                            "template": {
                                "spec": {
                                    "containers": [
                                        {
                                            "name": "worker",
                                            "envFrom": [
                                                {"configMapRef": {"name": "pipeline"}},
                                                {"secretRef": {"name": "database"}},
                                                {"secretRef": {"name": "proxy"}},
                                                {
                                                    "secretRef": {
                                                        "name": "swegen-model-credentials-pooled"
                                                    }
                                                },
                                            ],
                                        }
                                    ]
                                }
                            }
                        }
                    }
                ),
                stderr="",
            )
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
            "get",
            "deployment/swegen-generate",
            "-o=json",
        ],
        [
            "kubectl",
            "--request-timeout=10s",
            "-n",
            "swegen-pipeline",
            "get",
            "deployment/swegen-generate-overflow",
            "-o=json",
        ],
        [
            "kubectl",
            "--request-timeout=10s",
            "-n",
            "swegen-pipeline",
            "set",
            "image",
            "deployment/swegen-generate-overflow",
            "worker=swegen-worker:working",
        ],
        [
            "kubectl",
            "--request-timeout=10s",
            "-n",
            "swegen-pipeline",
            "patch",
            "deployment/swegen-generate-overflow",
            "--type=json",
            "-p",
            json.dumps(
                [
                    {
                        "op": "replace",
                        "path": "/spec/template/spec/containers/0/envFrom/3/secretRef/name",
                        "value": "swegen-model-credentials-sol-direct",
                    }
                ]
            ),
        ],
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


def test_scaler_does_not_touch_overflow_image_when_generate_fits_main_pool() -> None:
    from swegen.dashboard.server import K3sScaler

    commands: list[list[str]] = []

    def runner(command: list[str], **_kwargs: object) -> CompletedProcess[str]:
        commands.append(command)
        return CompletedProcess(command, 0, stdout="scaled", stderr="")

    K3sScaler(runner=runner).scale("generate", 80, max_replicas=768)

    assert all("set" not in command and "get" not in command for command in commands)


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
