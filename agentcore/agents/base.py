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

from agentcore.config import ChatConfig


class BaseAgent:
    """Shared LLM call infrastructure. Not instantiated directly."""

    def __init__(
        self, client: Anthropic, chat_cfg: ChatConfig, verbose: bool = True, *, model: str,
    ) -> None:
        self.client = client
        self.chat_cfg = chat_cfg
        self.verbose = verbose
        self.model = model

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

        kwargs = dict(model=self.model, max_tokens=self.chat_cfg.max_tokens, system=system, messages=messages)
        if tools:
            kwargs["tools"] = tools

        max_retries = self.chat_cfg.max_retries
        retry_delay = self.chat_cfg.retry_delay
        for attempt in range(1, max_retries + 1):
            try:
                response = self.client.messages.create(**kwargs)
                self._log_usage(response)
                return response
            except APIStatusError as e:
                if e.status_code in (429, 529) and attempt < max_retries:
                    delay = retry_delay * attempt
                    if self.verbose:
                        print(f"    [API] {e.status_code} — retrying in {delay}s ({attempt}/{max_retries})")
                    time.sleep(delay)
                else:
                    raise

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
