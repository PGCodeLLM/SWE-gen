from __future__ import annotations

from pathlib import Path

from reward_hacking_detector.hacking import load_llm_configs


def test_hacking_checker_reads_nested_swegen_toml_config(tmp_path: Path):
    config = tmp_path / "swegen.toml"
    config.write_text(
        """
[hacking]
[[hacking.llm]]
name = "checker"
endpoint = "https://checker.example"
model = "checker-model"
api_key = "checker-key"
""".strip()
        + "\n"
    )

    configs = load_llm_configs(config)

    assert len(configs) == 1
    assert configs[0].name == "checker"
    assert configs[0].model == "checker-model"
