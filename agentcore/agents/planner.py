"""PlannerAgent: translates a TaskHandoff into a structured BusinessPlan.

Knows:     ontology, business rules
Does NOT know: physical schema, SQL, conversation history, persona

The Planner is the authority on what a plan should contain.
It encodes all relevant business rules as Preconditions in the plan.
It uses ontology entity names — never table names.
"""

from anthropic import Anthropic

from agentcore.agents.base import BaseAgent
from agentcore.domain import DomainConfig
from agentcore.plan import BusinessPlan, TaskHandoff


class PlannerAgent(BaseAgent):
    """Translates a distilled task description into a structured BusinessPlan.

    Single LLM invocation per plan request — no conversation history,
    no schema knowledge, no persona.
    """

    def __init__(
        self,
        client: Anthropic,
        domain: DomainConfig,
        verbose: bool = True,
    ) -> None:
        super().__init__(client, verbose)
        self.domain = domain
        self._build_system_prompt()

    def _build_system_prompt(self) -> None:
        """Compose the system prompt from ontology and business rules only.

        Includes:  ontology (entity names + semantics), business rules
        Excludes:  physical schema, persona, conversation history
        """
        raise NotImplementedError

    def plan(self, handoff: TaskHandoff) -> BusinessPlan:
        """Translate a TaskHandoff into a BusinessPlan.

        Single LLM call — no agentic loop needed here.
        The model is given the handoff task + known entities and must emit
        a valid BusinessPlan JSON, which is parsed and returned as a dataclass.

        Raises:
            ValueError: if the model output cannot be parsed as a valid BusinessPlan.
        """
        raise NotImplementedError

    def _parse_plan(self, raw: str) -> BusinessPlan:
        """Parse the model's JSON output into a BusinessPlan dataclass.

        Validates that all required fields are present and entity names
        are non-empty strings. Does not validate against the ontology —
        that is the Executor's auditor responsibility.
        """
        raise NotImplementedError
