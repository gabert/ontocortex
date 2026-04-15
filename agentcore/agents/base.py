"""BaseAgent: shared LLM invocation logic used by all three agents.

Responsibilities:
- Single place for retry logic (429 / 529 handling)
- Prompt caching setup (ephemeral, 5-min TTL)
- Verbose token usage logging
- Text extraction from responses

Agents never import each other — they import this module only.
"""

import time
from datetime import datetime

from anthropic import Anthropic, APIStatusError

_MODEL = "claude-sonnet-4-6"
_MAX_TOKENS = 8192
_MAX_RETRIES = 3
_RETRY_DELAY = 5  # seconds


class BaseAgent:
    """Shared LLM call infrastructure. Not instantiated directly."""

    def __init__(self, client: Anthropic, verbose: bool = True) -> None:
        self.client = client
        self.verbose = verbose

    def _call_api(
        self,
        system_prompt: str,
        messages: list[dict],
        tools: list[dict] | None = None,
        cache_system: bool = True,
    ):
        """Call the Claude API with retry on transient errors.

        Args:
            system_prompt: The agent's system prompt (cached if cache_system=True).
            messages:       Conversation messages for this invocation.
            tools:          Tool definitions to expose to the model, or None.
            cache_system:   Whether to apply ephemeral cache_control to the system prompt.

        Returns:
            The raw Anthropic response object.
        """
        system_block = {"type": "text", "text": system_prompt}
        if cache_system:
            system_block["cache_control"] = {"type": "ephemeral"}

        now = datetime.now().strftime("%Y-%m-%d %H:%M (%A)")
        system = [system_block, {"type": "text", "text": f"Current date and time: {now}"}]

        kwargs = dict(model=_MODEL, max_tokens=_MAX_TOKENS, system=system, messages=messages)
        if tools:
            kwargs["tools"] = tools

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                response = self.client.messages.create(**kwargs)
                self._log_usage(response)
                return response
            except APIStatusError as e:
                if e.status_code in (429, 529) and attempt < _MAX_RETRIES:
                    delay = _RETRY_DELAY * attempt
                    if self.verbose:
                        print(f"    [API] {e.status_code} — retrying in {delay}s ({attempt}/{_MAX_RETRIES})")
                    time.sleep(delay)
                else:
                    raise

    def _call_api_with_tools(
        self,
        system_prompt: str,
        messages: list[dict],
        tools: list[dict],
        max_iterations: int = 10,
    ) -> tuple[str, list[dict]]:
        """Run the agentic tool-use loop until a final text response is produced.

        Subclasses must override _run_tools() to handle tool execution.

        Returns:
            (final_text, updated_messages)
        """
        for _ in range(max_iterations):
            response = self._call_api(system_prompt, messages, tools=tools)

            if response.stop_reason == "tool_use":
                messages.append({"role": "assistant", "content": response.content})
                messages.append({"role": "user", "content": self._run_tools(response)})
                continue

            text = self._extract_text(response)
            messages.append({"role": "assistant", "content": text})
            return text, messages

        # Exhausted iterations — return whatever text we have
        text = self._extract_text(response)
        messages.append({"role": "assistant", "content": text})
        return text, messages

    def _run_tools(self, response) -> list[dict]:
        """Execute tool calls from a response. Override in subclasses that use tools."""
        raise NotImplementedError("Subclass must implement _run_tools")

    @staticmethod
    def _extract_text(response) -> str:
        """Extract the plain text from a final (non-tool-use) response."""
        return next((b.text for b in response.content if hasattr(b, "text")), "")

    def _log_usage(self, response) -> None:
        """Print token usage if verbose."""
        if not self.verbose:
            return
        u = response.usage
        cached = getattr(u, "cache_read_input_tokens", 0) or 0
        written = getattr(u, "cache_creation_input_tokens", 0) or 0
        if cached or written:
            print(f"    [CACHE] read={cached}, written={written}, input={u.input_tokens}")
