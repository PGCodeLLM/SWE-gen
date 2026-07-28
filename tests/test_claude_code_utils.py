from swegen.create.claude_code_utils import redact_sensitive_text


def test_redact_sensitive_text_removes_proxy_userinfo_and_tokens() -> None:
    text = (
        "proxy=http://user:p%40ss@proxy.example:8080 "
        "github=ghp_abcdefghijklmnopqrstuvwxyz "
        "fine=github_pat_abcdefghijklmnopqrstuvwxyz "
        "openai=sk-abcdefghijklmnopqrstuvwxyz "
        "Authorization: Bearer arbitrary-token.value "
        "OPENAI_API_KEY=plain-secret "
        "ANTHROPIC_AUTH_TOKEN='quoted-secret' "
        "{'GITHUB_TOKEN': 'mapping-secret'}"
    )

    redacted = redact_sensitive_text(text)

    assert "user" not in redacted
    assert "p%40ss" not in redacted
    assert "ghp_" not in redacted
    assert "github_pat_" not in redacted
    assert "sk-" not in redacted
    assert "arbitrary-token.value" not in redacted
    assert "plain-secret" not in redacted
    assert "quoted-secret" not in redacted
    assert "mapping-secret" not in redacted
    assert "http://<REDACTED>@proxy.example:8080" in redacted
