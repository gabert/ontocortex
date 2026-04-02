"""ConversationAgent: manages user dialogue and delegates tasks to the Planner.

Knows:     ontology, business rules, persona, conversation history
Does NOT know: physical schema, SQL, BusinessPlan internals

Delegation mechanism:
- Has exactly one tool: submit_plan_request(task, known_entities)
- When it calls this tool, the Pipeline intercepts, runs Planner → Executor,
  and injects the PlanResult back as a tool result
- The agent never sees SQL — only business-language outcomes
"""

from anthropic import Anthropic

from agentcore.agents.base import BaseAgent
from agentcore.domain import DomainConfig
from agentcore.plan import TaskHandoff

# Tool definition exposed to the ConversationAgent.
# The Pipeline is responsible for handling calls to this tool — it never
# reaches an actual execute() function in a tools registry.
_SUBMIT_PLAN_REQUEST_TOOL = {
    "name": "submit_plan_request",
    "description": (
        "Delegate a data retrieval or modification task to the back-end planning system. "
        "Call this whenever the user asks you to look up, create, update, or delete information. "
        "Provide a plain-English description of the task and any entity identifiers already known."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "task": {
                "type": "string",
                "description": "Plain-English description of what needs to be done.",
            },
            "known_entities": {
                "type": "object",
                "description": (
                    "Entity identifiers already provided by the user. "
                    "Keys are entity names (e.g. 'policy', 'client'), "
                    "values are the known IDs or field values."
                ),
            },
        },
        "required": ["task"],
    },
}


class ConversationAgent(BaseAgent):
    """Manages the user-facing conversation turn by turn.

    The pipeline calls chat() for each user message. If the agent decides to
    delegate, chat() returns a TaskHandoff instead of a final text response.
    The pipeline resolves the handoff and calls chat() again with the result
    injected as a tool response.
    """

    def __init__(
        self,
        client: Anthropic,
        domain: DomainConfig,
        verbose: bool = True,
    ) -> None:
        super().__init__(client, verbose)
        self.domain = domain
        self.messages: list[dict] = []
        self._build_system_prompt()

    def _build_system_prompt(self) -> None:
        """Compose the system prompt from domain persona, ontology, and business rules.

        Does NOT include physical schema or SQL rules.
        """
        raise NotImplementedError

    def chat(self, user_message: str) -> tuple[str | None, TaskHandoff | None]:
        """Process one user message.

        Returns one of two states:
        - (response_text, None)  — agent produced a final answer; pipeline returns it to user
        - (None, TaskHandoff)    — agent called submit_plan_request; pipeline must resolve
                                   and call inject_plan_result() then chat() with no new message

        The conversation history is maintained internally.
        """
        raise NotImplementedError

    def inject_plan_result(self, tool_use_id: str, result_text: str) -> None:
        """Inject a PlanResult (as text) back into the conversation as a tool result.

        Called by the pipeline after Planner → Executor completes.
        After this, call chat() with user_message=None to get the final response.
        """
        raise NotImplementedError

    def reset(self) -> None:
        """Clear conversation history to start a fresh session."""
        self.messages.clear()
