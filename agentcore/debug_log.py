"""Deterministic conversation logger + on-error LLM analyzer.

Every turn dumps a JSON snapshot of the full conversation state to
`logs/session_<timestamp>.jsonl`. On exception, the same dump plus the
traceback is sent to Claude for an automated post-mortem written to
`logs/error_<timestamp>.md`.

Logging is best-effort: failures here never raise into the caller.
"""

from __future__ import annotations

import json
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"

_ANALYZER_MODEL = "claude-sonnet-4-5"
_ANALYZER_SYSTEM = """\
You are debugging a multi-turn conversation between a user, a Claude-based
conversation agent, and a deterministic SIF-to-SQL pipeline.

You will receive:
1. The full message history (system prompt omitted, tool_use / tool_result blocks intact).
2. The query log (SQL emitted, DB results / errors).
3. The Python traceback that triggered the analysis.

Identify the root cause in plain language. Be specific: cite message
indices, tool_use ids, or SQL fragments. Suggest the smallest fix.
Keep the response under 400 words.
"""


def _serialize(obj: Any) -> Any:
    """Best-effort JSON-safe conversion for Anthropic SDK objects."""
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialize(v) for v in obj]
    if hasattr(obj, "model_dump"):
        try:
            return obj.model_dump()
        except Exception:
            pass
    if hasattr(obj, "__dict__"):
        return {k: _serialize(v) for k, v in vars(obj).items() if not k.startswith("_")}
    return repr(obj)


def _ensure_log_dir() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    return LOG_DIR


def session_log_path(session_id: str) -> Path:
    return _ensure_log_dir() / f"session_{session_id}.jsonl"


def new_session_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def dump_turn(
    session_id: str,
    user_message: str | None,
    messages: list[dict],
    query_log: list[dict],
    response: str | None,
    error: BaseException | None = None,
) -> Path | None:
    """Append a JSON line describing one pipeline turn. Never raises."""
    try:
        record = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "user_message": user_message,
            "messages": _serialize(messages),
            "query_log": _serialize(query_log),
            "response": response,
            "error": None,
        }
        if error is not None:
            record["error"] = {
                "type": type(error).__name__,
                "message": str(error),
                "traceback": "".join(
                    traceback.format_exception(type(error), error, error.__traceback__)
                ),
            }
        path = session_log_path(session_id)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
        return path
    except Exception:
        return None


def analyze_error(
    client,
    session_id: str,
    error: BaseException,
    messages: list[dict],
    query_log: list[dict],
) -> Path | None:
    """Send the conversation context + error to Claude for a post-mortem.

    Returns the analysis file path, or None if the call failed.
    """
    try:
        tb = "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )
        payload = {
            "messages": _serialize(messages),
            "query_log": _serialize(query_log),
            "error": {"type": type(error).__name__, "message": str(error), "traceback": tb},
        }
        prompt = (
            "Here is the full debugging payload as JSON. "
            "Analyze the failure.\n\n"
            "```json\n"
            + json.dumps(payload, default=str, ensure_ascii=False, indent=2)
            + "\n```"
        )
        resp = client.messages.create(
            model=_ANALYZER_MODEL,
            max_tokens=2048,
            system=_ANALYZER_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
        text = "".join(b.text for b in resp.content if getattr(b, "type", None) == "text")
        out = _ensure_log_dir() / f"error_{session_id}_{datetime.now().strftime('%H%M%S')}.md"
        out.write_text(
            f"# Error analysis — session {session_id}\n\n"
            f"**Error:** `{type(error).__name__}: {error}`\n\n"
            f"## Analysis\n\n{text}\n\n"
            f"## Traceback\n\n```\n{tb}```\n",
            encoding="utf-8",
        )
        return out
    except Exception:
        return None
