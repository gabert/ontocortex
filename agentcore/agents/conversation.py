"""ConversationAgent: owns the full tool-use loop.

Knows: ontology, business rules, persona, SIF format, conversation history.
Does NOT know: physical schema, SQL, database structure.

The agent drives the loop: user message → LLM → (tool_use → tool_executor → tool_result → LLM)* → text.
The pipeline passes in a tool_executor callback that runs SIF operations and
returns a result string. Every tool_use block in every assistant message is
paired with its own tool_result, so the message history is always a valid
Anthropic conversation.
"""

from typing import Callable

from anthropic import Anthropic

from agentcore.agents.base import BaseAgent
from agentcore.domain import DomainConfig

ToolExecutor = Callable[[list[dict]], tuple[str, list[dict]]]

_DEFAULT_MAX_ITERATIONS = 20

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

5. **link** / **unlink** — attach or detach two existing entities across a
   many-to-many relation (one that traverses a junction table). Use these
   *after* both entities exist. Do not try to create a junction row with a
   plain create.
   - relation: the ontology relationship name (must be many-to-many)
   - from: {{entity: ClassName, filters: {{...}}}} — locates one side
   - to:   {{entity: ClassName, filters: {{...}}}} — locates the other side
   Both filter sets must narrow to exactly one row.

Batching & transactions:

Every submit_sif call runs as a single database transaction. If any op in
the batch fails, the whole batch is rolled back — you will see a clear
"rolled back" notice in the error text. Group related writes in one call
when they must all succeed together (e.g. create a Cosigner + link it to
a Loan).

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

Create + link in one transactional batch (add a new cosigner to an existing loan):
  submit_sif(operations=[
    {{"op": "create", "entity": "Cosigner",
      "data": {{"first_name": "Jane", "last_name": "Doe", "email": "jane@x.org", "credit_score": 720}}}},
    {{"op": "link", "relation": "hasCosigner",
      "from": {{"entity": "Loan",     "filters": {{"loan_number": "SLN-2026-00001"}}}},
      "to":   {{"entity": "Cosigner", "filters": {{"email": "jane@x.org"}}}}}}
  ])

You will receive the actual data as a tool response. Use it to answer the user.
Never invent data — only use what the tool response gives you.
Never mention SQL, tables, databases, or technical details to the user.

=== HANDLING TOOL ERRORS ===

When submit_sif returns an error (validation failure, database constraint
violation, translation error, etc.), read it carefully and decide:

1. **Correct and retry** — if the error tells you exactly what to fix (e.g. a
   wrong field name or value), issue a new submit_sif call with the correction.
   Never retry with the same values — the same inputs will produce the same error.

2. **Ask the user** — if the error reveals missing information (e.g. a required
   field the user hasn't provided yet, or an ambiguous reference), ask the user
   a concrete question in plain language.

3. **Try a different approach** — if the operation is conceptually wrong (e.g.
   a duplicate record already exists), look it up instead, or propose an
   alternative to the user.

After the maximum of {max_iter} tool iterations you must respond with text.
Never go silent. Always produce a final message for the user on every turn,
even if you could not complete the request — explain what went wrong in
plain language.
"""


class ConversationAgent(BaseAgent):
    """Owns the Anthropic tool-use loop for one conversation."""

    def __init__(
        self,
        client: Anthropic,
        domain: DomainConfig,
        sif_tool: dict,
        verbose: bool = True,
        max_iterations: int = _DEFAULT_MAX_ITERATIONS,
    ) -> None:
        super().__init__(client, verbose)
        self.domain = domain
        self.sif_tool = sif_tool
        self.max_iterations = max_iterations
        self.messages: list[dict] = []
        self.system_prompt = self._build_system_prompt()

    def _build_system_prompt(self) -> str:
        ontology = self.domain.ontology_compact_text or self.domain.ontology_text
        return _SYSTEM_TEMPLATE.format(
            persona=self.domain.prompt_text,
            ontology=ontology,
            rules=self.domain.rules_text,
            max_iter=self.max_iterations,
        )

    def chat(
        self, user_message: str, tool_executor: ToolExecutor,
    ) -> tuple[str, list[dict]]:
        """Run one full turn of the tool-use loop.

        Args:
            user_message:  The new user message.
            tool_executor: Callback that runs a list of SIF operations and
                returns (result_text, per_call_query_log). Must never raise —
                errors should be encoded into result_text so the agent can
                self-correct.

        Returns:
            (final_text, combined_query_log)
        """
        self.messages.append({"role": "user", "content": user_message})
        query_log: list[dict] = []

        for _ in range(self.max_iterations):
            response = self._call_api(
                self.system_prompt, self.messages, tools=[self.sif_tool],
            )

            if response.stop_reason != "tool_use":
                text = self._extract_text(response)
                self.messages.append({"role": "assistant", "content": text})
                return text, query_log

            # Assistant turn: convert SDK blocks to plain dicts so history is
            # always JSON-serializable (cleaner debug logs, no SDK coupling).
            assistant_content = _blocks_to_dicts(response.content)
            self.messages.append({"role": "assistant", "content": assistant_content})

            # Execute every tool_use block and pair each with its own result.
            # This is the invariant that keeps the conversation valid for the
            # next API call.
            tool_results: list[dict] = []
            for block in response.content:
                if block.type != "tool_use":
                    continue

                if block.name == "submit_sif":
                    operations = block.input.get("operations", []) or []
                    result_text, block_log = tool_executor(operations)
                    query_log.extend(block_log)
                else:
                    result_text = f"Unknown tool: {block.name}"

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result_text,
                })

            self.messages.append({"role": "user", "content": tool_results})

        # Iteration cap: force a final text message so the conversation stays
        # valid and the user sees something instead of a hang.
        fallback = (
            "I tried several approaches but couldn't complete that request. "
            "Could you rephrase it or break it into smaller steps?"
        )
        self.messages.append({"role": "assistant", "content": fallback})
        if self.verbose:
            print(f"    [CONVERSATION] iteration cap ({self.max_iterations}) hit")
        return fallback, query_log

    def reset(self) -> None:
        self.messages.clear()


def _blocks_to_dicts(content) -> list[dict]:
    """Convert Anthropic SDK content blocks to plain dicts."""
    out: list[dict] = []
    for block in content:
        if block.type == "text":
            out.append({"type": "text", "text": block.text})
        elif block.type == "tool_use":
            out.append({
                "type": "tool_use",
                "id": block.id,
                "name": block.name,
                "input": block.input,
            })
    return out
