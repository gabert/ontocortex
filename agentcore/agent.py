"""Session-based domain agent powered by Claude and PostgreSQL."""

import time
from datetime import datetime

from anthropic import Anthropic, APIStatusError

from agentcore.config import AppConfig
from agentcore.domain import DomainConfig
from agentcore.schema import build_schema_description, build_validation_spec
from agentcore.tools import FRAMEWORK_TOOLS, execute_tool

_MODEL          = "claude-sonnet-4-20250514"
_MAX_TOKENS     = 8192
_MAX_ITERATIONS = 10
_MAX_RETRIES    = 3
_RETRY_DELAY    = 5  # seconds

_SYSTEM_TEMPLATE = """\
{domain_prompt}

=== DOMAIN MODEL (ontology -- what things mean) ===

{ontology}

=== DATABASE SCHEMA (internal -- never expose to the user) ===

{schema}

=== BUSINESS RULES ===

{rules}

=== VALIDATION RULES (internal -- enforced by the framework before any INSERT) ===

{validation}

If the framework returns a validation error from execute_sql, inform the user clearly and
ask for the missing or corrected value before retrying.

=== EXECUTION PLANNING (mandatory for all write operations) ===

Before executing any operation that involves INSERT, UPDATE, or DELETE:

1. **State your plan** in your text response to the user. List:
   - Which tables will be affected and in what order
   - What data you already have from the user
   - What data you still need to collect
   - Which FK dependencies must exist before you can proceed

2. **Call get_action_plan** for each table you plan to INSERT into.
   Use the returned field list to collect all required values from the user
   before attempting the INSERT.

3. **Execute in dependency order** — parent records before child records.
   If a step fails, stop and inform the user rather than continuing.

Do not skip the planning step even if the user seems to have provided enough information.
For simple SELECT queries, no plan is needed — just execute.

=== SQL RULES (mandatory) ===

1. Never embed literal values in SQL. Always use :name placeholders and pass values in params.
   Correct:   SELECT * FROM clients WHERE email = :email   params: {{"email": "john@example.com"}}
   Incorrect: SELECT * FROM clients WHERE email = 'john@example.com'

"""


class DomainAgent:
    """A session-based agent that loads its persona and schema from a domain manifest."""

    def __init__(self, config: AppConfig, domain: DomainConfig, verbose: bool = True) -> None:
        self.config  = config
        self.domain  = domain
        self.verbose = verbose
        self.client  = Anthropic(api_key=config.api_key)

        schema_data = domain.schema_data
        schema = build_schema_description(schema_data)
        self.validation_spec, validation_text = build_validation_spec(schema_data)

        ontology = domain.ontology_compact_text or domain.ontology_text
        self.system_prompt = _SYSTEM_TEMPLATE.format(
            domain_prompt=domain.prompt_text,
            ontology=ontology,
            schema=schema,
            rules=domain.rules_text,
            validation=validation_text,
        )
        self.messages: list[dict] = []
        self.last_query_log: list[dict] = []

    def chat(self, user_message: str) -> str:
        """Send a message and get the agent's conversational response."""
        self.messages.append({"role": "user", "content": user_message})
        self.last_query_log = []
        loop_start = len(self.messages)

        try:
            assistant_message = ""
            for _ in range(_MAX_ITERATIONS):
                response = self._call_api()

                if response.stop_reason == "tool_use":
                    self.messages.append({"role": "assistant", "content": response.content})
                    self.messages.append({"role": "user", "content": self._run_tools(response)})
                    continue

                assistant_message = self._extract_text(response)
                self.messages.append({"role": "assistant", "content": assistant_message})
                break

            # Collapse intermediate tool/result messages — keep only the final answer
            self.messages[loop_start:] = [{"role": "assistant", "content": assistant_message}]
            return assistant_message
        except Exception:
            # Remove intermediate tool messages so the conversation stays valid
            del self.messages[loop_start:]
            raise

    def _run_tools(self, response) -> list[dict]:
        """Execute all tool_use blocks in a response and return tool_result list."""
        context = {
            "db_config":       self.config.database,
            "validation_spec": self.validation_spec,
            "verbose":         self.verbose,
            "query_log":       self.last_query_log,
        }
        return [
            {
                "type":        "tool_result",
                "tool_use_id": block.id,
                "content":     execute_tool(block.name, block.input, context),
            }
            for block in response.content
            if block.type == "tool_use"
        ]

    @staticmethod
    def _extract_text(response) -> str:
        """Extract the text content from a final response."""
        return next((b.text for b in response.content if hasattr(b, "text")), "")

    def reset(self) -> None:
        """Clear conversation history to start a fresh session."""
        self.messages.clear()

    def _call_api(self):
        """Call the Claude API with retry on transient errors (429, 529).

        Uses prompt caching: the system prompt and tools are cached across turns
        (ephemeral, 5-min TTL). The datetime is a separate uncached block so it
        doesn't invalidate the cache.
        """
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                now = datetime.now().strftime("%Y-%m-%d %H:%M (%A)")
                response = self.client.messages.create(
                    model=_MODEL,
                    max_tokens=_MAX_TOKENS,
                    system=[
                        {
                            "type": "text",
                            "text": self.system_prompt,
                            "cache_control": {"type": "ephemeral"},
                        },
                        {
                            "type": "text",
                            "text": f"Current date and time: {now}",
                        },
                    ],
                    messages=self.messages,
                    tools=FRAMEWORK_TOOLS,
                )
                if self.verbose:
                    u = response.usage
                    cached = getattr(u, "cache_read_input_tokens", 0) or 0
                    written = getattr(u, "cache_creation_input_tokens", 0) or 0
                    if cached or written:
                        print(f"    [CACHE] read={cached}, written={written}, input={u.input_tokens}")
                return response
            except APIStatusError as e:
                if e.status_code in (429, 529) and attempt < _MAX_RETRIES:
                    delay = _RETRY_DELAY * attempt
                    if self.verbose:
                        print(f"    [API] {e.status_code} — retrying in {delay}s ({attempt}/{_MAX_RETRIES})")
                    time.sleep(delay)
                else:
                    raise
