"""AgentPipeline: wires the three agents into the public chat interface.

This is the ONLY module that imports all three agents.
main.py imports only this module.

Flow per user message:
    1. ConversationAgent.chat(user_message)
       → if (response, None):  return response to caller
       → if (None, handoff):   continue to step 2

    2. PlannerAgent.plan(handoff) → BusinessPlan

    3. ExecutorAgent.execute(plan) → PlanResult

    4. ConversationAgent.inject_plan_result(tool_use_id, result_text)
       ConversationAgent.chat(user_message=None)  ← resumes with tool result injected
       → return response to caller
"""

from anthropic import Anthropic

from agentcore.agents.conversation import ConversationAgent
from agentcore.agents.executor import ExecutorAgent
from agentcore.agents.planner import PlannerAgent
from agentcore.config import AppConfig
from agentcore.domain import DomainConfig
from agentcore.schema import build_schema_description, build_validation_spec


class AgentPipeline:
    """Public interface for the three-agent system.

    Drop-in replacement for DomainAgent: same chat() and reset() surface.
    """

    def __init__(
        self,
        config: AppConfig,
        domain: DomainConfig,
        verbose: bool = True,
    ) -> None:
        client = Anthropic(api_key=config.api_key)

        schema_data = domain.schema_data
        validation_spec, _ = build_validation_spec(schema_data)

        self._conversation = ConversationAgent(client, domain, verbose)
        self._planner = PlannerAgent(client, domain, verbose)
        self._executor = ExecutorAgent(
            client, domain, config.database, validation_spec, verbose
        )

    def chat(self, user_message: str) -> str:
        """Send a user message through the full pipeline and return the final response.

        Handles the full delegation loop:
        - If ConversationAgent delegates, runs Planner → Executor and injects result.
        - Returns the final natural-language response from ConversationAgent.
        """
        raise NotImplementedError

    def reset(self) -> None:
        """Clear conversation history. Planner and Executor are stateless."""
        self._conversation.reset()