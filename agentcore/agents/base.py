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

_MODEL = "claude-sonnet-4-20250514"
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
        raise NotImplementedError

    def _call_api_with_tools(
        self,
        system_prompt: str,
        messages: list[dict],
        tools: list[dict],
        max_iterations: int = 10,
    ) -> tuple[str, list[dict]]:
        """Run the agentic tool-use loop until a final text response is produced.

        Args:
            system_prompt:  System prompt for this agent.
            messages:       Initial messages (modified in-place).
            tools:          Tools available in this loop.
            max_iterations: Hard cap on tool rounds.

        Returns:
            (final_text, updated_messages)
        """
        raise NotImplementedError

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
