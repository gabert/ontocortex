"""AgentPipeline: thin wrapper around ConversationAgent + deterministic SIF executor.

The pipeline is now just three things:
  1. Hold the SchemaMap, config, and ConversationAgent.
  2. Provide a tool_executor callback that runs SIF operations against the DB.
  3. Snapshot/rollback + debug-log every turn so transient failures never
     corrupt conversation history.

All agentic reasoning and self-correction live in ConversationAgent.
"""

from anthropic import Anthropic

from agentcore.agents.conversation import ConversationAgent
from agentcore.config import AppConfig
from agentcore.debug_log import analyze_error, dump_turn, new_session_id
from agentcore.domain import DomainConfig
from agentcore.actions import clear_actions, load_domain_actions
from agentcore.sif import SchemaMap, build_sif_tool
from agentcore.sif_sql import execute_sif


class AgentPipeline:
    """SIF-based pipeline: LLM produces intent, code executes it."""

    def __init__(
        self, config: AppConfig, domain: DomainConfig, verbose: bool = True,
    ) -> None:
        self.config = config
        self.domain = domain
        self.verbose = verbose

        # Build the ontology-to-schema mapping once.
        self.schema_map = SchemaMap(domain.ontology_model, domain.schema_data)

        # Load domain-specific actions before building the tool schema.
        clear_actions()
        n = load_domain_actions(domain.ontology_path.parent)
        if n and verbose:
            print(f"    [PIPELINE] Loaded {n} domain action(s)")

        # Tool schema with entity/relation/action enums injected.
        sif_tool = build_sif_tool(self.schema_map)

        self._client = Anthropic(api_key=config.api_key)
        self.conversation = ConversationAgent(
            self._client, domain, sif_tool, verbose=verbose,
        )

        # Exposed for the UI: query log from the most recent turn.
        self.last_query_log: list[dict] = []

        # Per-session debug log file id.
        self.session_id = new_session_id()

    def chat(self, user_message: str) -> str:
        """Send a user message through the pipeline. Returns the final text."""
        self.last_query_log = []

        # Snapshot: if anything raises, we roll the conversation back to this
        # exact length. This makes partial turns impossible and guarantees the
        # history is always a valid Anthropic conversation.
        snapshot_len = len(self.conversation.messages)

        def tool_executor(operations: list[dict]) -> tuple[str, list[dict]]:
            """Run one SIF batch. Must never raise — errors become text."""
            if self.verbose and operations:
                summary = ", ".join(
                    f"{op.get('op')} {op.get('entity', op.get('action', ''))}"
                    for op in operations
                )
                print(f"    [PIPELINE] SIF: {summary}")

            result_text, query_log = execute_sif(
                operations, self.schema_map, self.config.database, self.verbose,
            )
            self.last_query_log.extend(query_log)
            return result_text, query_log

        try:
            response, _ = self.conversation.chat(user_message, tool_executor)

            dump_turn(
                self.session_id, user_message,
                self.conversation.messages, self.last_query_log,
                response, error=None,
            )
            return response

        except Exception as e:
            # Capture the corrupted state for post-mortem analysis BEFORE the
            # rollback, so the dump reflects exactly what blew up.
            corrupted = list(self.conversation.messages)
            dump_turn(
                self.session_id, user_message,
                corrupted, self.last_query_log,
                response=None, error=e,
            )
            try:
                path = analyze_error(
                    self._client, self.session_id, e,
                    corrupted, self.last_query_log,
                )
                if self.verbose and path:
                    print(f"    [PIPELINE] Error analysis → {path}")
            except Exception:
                pass

            # Atomic rollback: history length goes back to the pre-turn snapshot.
            # No orphaned tool_use blocks are possible by construction.
            del self.conversation.messages[snapshot_len:]
            raise

    def reset(self) -> None:
        """Clear conversation history."""
        self.conversation.reset()
        self.last_query_log = []
