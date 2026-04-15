"""ConversationAgent: manages user dialogue and emits SIF operations.

Knows:     ontology, business rules, persona, SIF format, conversation history
Does NOT know: physical schema, SQL, database structure

Delegation mechanism:
- Has exactly one tool: submit_sif(operations)
- When it calls this tool, the Pipeline translates SIF to SQL deterministically,
  executes it, and injects the data back as a tool result
- The agent never sees SQL or table names — only ontology concepts
"""

from anthropic import Anthropic

from agentcore.agents.base import BaseAgent
from agentcore.domain import DomainConfig

_SYSTEM_TEMPLATE = """\
{persona}

=== DOMAIN MODEL (ontology — shared vocabulary) ===

{ontology}

=== BUSINESS RULES ===

{rules}

=== SIF (Structured Intent Format) ===

You have one tool: submit_sif. Use it whenever the user asks you to look up,
create, update, or delete any data. You express intent using ontology concepts.

**Entity names** must be ontology class names (e.g. Customer, Policy, Vehicle, Pet).
**Property names** must be ontology property names (e.g. first_name, status, claim_amount).
**Relationship names** must be ontology relationship names (e.g. hasPolicy, coversVehicle).

Operations:

1. **query** — read data
   - filters: {{property: value}} for equality matching on the main entity
   - relations: chain of joins to traverse. Each has rel (relationship name),
     entity (class on the other end), and optional filters on that entity.
   - aggregate: {{fn: count/sum/avg/min/max, field: property}} for aggregations
   - fields: list of properties to return (omit for all)
   - sort: {{field: property, dir: asc/desc}}
   - limit: max rows

2. **create** — insert new entity
   - data: {{property: value}} for the new record
   - resolve: {{relationship_name: {{entity: ClassName, filters: {{prop: value}}}}}}
     to look up related entities for FK values

3. **update** — modify existing entity
   - filters: identify which record(s) to update
   - data: {{property: new_value}} fields to change

4. **delete** — remove entity
   - filters: identify which record(s) to delete

Examples:

Query with relation traversal (find vehicles for a customer):
  submit_sif(operations=[{{
    "op": "query", "entity": "Vehicle",
    "relations": [
      {{"rel": "coversVehicle", "entity": "Policy"}},
      {{"rel": "hasPolicy", "entity": "Customer", "filters": {{"first_name": "John", "last_name": "Smith"}}}}
    ]
  }}])

Aggregation (count active policies):
  submit_sif(operations=[{{
    "op": "query", "entity": "Policy",
    "filters": {{"status": "active"}},
    "aggregate": {{"fn": "count"}}
  }}])

Create with resolve (new claim on a policy):
  submit_sif(operations=[{{
    "op": "create", "entity": "Claim",
    "data": {{"claim_date": "2026-04-09", "claim_amount": 5000, "claim_type": "accident", "status": "claim_pending"}},
    "resolve": {{"hasClaim": {{"entity": "Policy", "filters": {{"policy_number": "POL-2024-00001"}}}}}}
  }}])

You will receive the actual data as a tool response. Use it to answer the user.
Never invent data — only use what the tool response gives you.
Never mention SQL, tables, databases, or technical details to the user.
"""


class ConversationAgent(BaseAgent):
    """Manages the user-facing conversation and emits SIF operations."""

    def __init__(
        self,
        client: Anthropic,
        domain: DomainConfig,
        sif_tool: dict,
        verbose: bool = True,
    ) -> None:
        super().__init__(client, verbose)
        self.domain = domain
        self.sif_tool = sif_tool
        self.messages: list[dict] = []
        self.system_prompt = self._build_system_prompt()
        self._pending_tool_use_id: str | None = None

    def _build_system_prompt(self) -> str:
        ontology = self.domain.ontology_compact_text or self.domain.ontology_text
        return _SYSTEM_TEMPLATE.format(
            persona=self.domain.prompt_text,
            ontology=ontology,
            rules=self.domain.rules_text,
        )

    def chat(self, user_message: str | None) -> tuple[str | None, list[dict] | None]:
        """Process one user message.

        Returns:
            (response_text, None)          — final answer for the user
            (None, sif_operations_list)    — agent wants data; pipeline must execute SIF
        """
        if user_message is not None:
            self.messages.append({"role": "user", "content": user_message})

        response = self._call_api(
            self.system_prompt, self.messages, tools=[self.sif_tool],
        )

        if response.stop_reason == "tool_use":
            self.messages.append({"role": "assistant", "content": response.content})

            for block in response.content:
                if block.type == "tool_use" and block.name == "submit_sif":
                    self._pending_tool_use_id = block.id
                    operations = block.input.get("operations", [])
                    return None, operations

        text = self._extract_text(response)
        self.messages.append({"role": "assistant", "content": text})
        return text, None

    def inject_result(self, result_text: str) -> None:
        """Inject execution result back into the conversation as a tool response."""
        if not self._pending_tool_use_id:
            raise RuntimeError("No pending tool call to inject result into")

        self.messages.append({
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": self._pending_tool_use_id,
                    "content": result_text,
                }
            ],
        })
        self._pending_tool_use_id = None

    def reset(self) -> None:
        self.messages.clear()
        self._pending_tool_use_id = None
