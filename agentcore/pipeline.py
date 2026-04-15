"""AgentPipeline: Conversation Agent + deterministic SIF translator.

Flow:
  User message -> ConversationAgent (LLM) -> SIF JSON -> validate -> translate (code) -> SQL -> DB -> data -> back to ConversationAgent

If SIF validation fails, the error is fed back to the conversation agent
as a tool result so it can correct and retry (up to MAX_RETRIES).

The only LLM calls are in the ConversationAgent. Translation and execution are deterministic.
"""

from anthropic import Anthropic

from agentcore.agents.conversation import ConversationAgent
from agentcore.config import AppConfig
from agentcore.domain import DomainConfig
from agentcore.sif import (
    SchemaMap,
    build_sif_tool,
    clear_actions,
    execute_sif,
    load_domain_actions,
)

_DEFAULT_MAX_RETRIES = 5


class AgentPipeline:
    """SIF-based pipeline: LLM produces intent, code executes it."""

    def __init__(
        self, config: AppConfig, domain: DomainConfig,
        verbose: bool = True, max_retries: int = _DEFAULT_MAX_RETRIES,
    ) -> None:
        self.max_retries = max_retries
        self.config = config
        self.domain = domain
        self.verbose = verbose

        # Build the ontology-to-schema mapping once. SchemaMap consumes a
        # structured ontology model (IRI-keyed) — no more regex parsing.
        self.schema_map = SchemaMap(domain.ontology_model, domain.schema_data)

        # Load domain-specific actions before building the tool schema
        clear_actions()
        n = load_domain_actions(domain.ontology_path.parent)
        if n and verbose:
            print(f"    [PIPELINE] Loaded {n} domain action(s)")

        # Tool schema with entity/relation/action enums injected — the model
        # literally cannot emit a name outside the current domain.
        sif_tool = build_sif_tool(self.schema_map)

        client = Anthropic(api_key=config.api_key)
        self.conversation = ConversationAgent(client, domain, sif_tool, verbose)

        # Expose query log for the UI
        self.last_query_log: list[dict] = []

    def chat(self, user_message: str) -> str:
        """Send a user message through the pipeline. Returns the final response text."""
        self.last_query_log = []

        try:
            response, operations = self.conversation.chat(user_message)

            retries = 0
            while operations is not None:
                if self.verbose:
                    ops_summary = ", ".join(
                        f"{op.get('op')} {op.get('entity', op.get('action', ''))}"
                        for op in operations
                    )
                    print(f"    [PIPELINE] SIF: {ops_summary}")

                success, result_text, query_log = execute_sif(
                    operations, self.schema_map, self.config.database, self.verbose,
                )
                self.last_query_log.extend(query_log)

                if self.verbose:
                    status = "OK" if success else "ERROR"
                    print(f"    [PIPELINE] Result: {status}")

                if success:
                    self.conversation.inject_result(result_text)
                    response, _ = self.conversation.chat(None)
                    break

                if retries >= self.max_retries:
                    if self.verbose:
                        print(f"    [PIPELINE] Retries exhausted, aborting")
                    self.conversation.inject_result(
                        "Unable to process this request. Please try rephrasing your question."
                    )
                    response, _ = self.conversation.chat(None)
                    break

                # Validation/translation failed — feed error back, let the agent retry
                retries += 1
                if self.verbose:
                    print(f"    [PIPELINE] Retry {retries}/{self.max_retries}")
                self.conversation.inject_result(result_text)
                response, operations = self.conversation.chat(None)

            return response or ""

        except Exception:
            # Clean up partial messages so conversation stays valid
            msgs = self.conversation.messages
            while msgs and msgs[-1]["role"] != "user":
                msgs.pop()
            if msgs:
                msgs.pop()
            raise

    def reset(self) -> None:
        """Clear conversation history."""
        self.conversation.reset()
        self.last_query_log = []
