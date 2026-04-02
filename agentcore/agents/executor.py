"""ExecutorAgent: validates a BusinessPlan and executes it as SQL.

Knows:     ontology, business rules (read-only auditor role), physical schema
Does NOT know: conversation history, persona

Two-phase execution:
1. Audit:   cross-check the plan against own ontology + business rules knowledge.
            If the plan references unknown entities or is missing obvious preconditions,
            reject before touching the DB.
2. Execute: evaluate Planner-encoded preconditions via SQL, then run operations
            inside a transaction (if plan.atomic=True).

Always returns PlanResult in business language — never surfaces SQL errors upward.

Journaling hook:
    _journal(plan, result) is called after every execution attempt.
    NOT IMPLEMENTED — stub exists as a design hook. Do not implement until requested.
"""

from agentcore.agents.base import BaseAgent
from agentcore.config import DatabaseConfig
from agentcore.domain import DomainConfig
from agentcore.plan import BusinessPlan, PlanResult
from anthropic import Anthropic


class ExecutorAgent(BaseAgent):
    """Validates and executes a BusinessPlan against the database.

    Has access to the physical schema via build_schema_description() and to
    ontology + business rules for the auditor cross-check.
    """

    def __init__(
        self,
        client: Anthropic,
        domain: DomainConfig,
        db_config: DatabaseConfig,
        validation_spec: dict,
        verbose: bool = True,
    ) -> None:
        super().__init__(client, verbose)
        self.domain = domain
        self.db_config = db_config
        self.validation_spec = validation_spec
        self._build_system_prompt()

    def _build_system_prompt(self) -> None:
        """Compose the system prompt from physical schema, ontology, and business rules.

        Includes:  physical schema (DDL), ontology, business rules (auditor context),
                   BusinessPlan JSON schema, SQL rules
        Excludes:  persona, conversation history
        """
        raise NotImplementedError

    def execute(self, plan: BusinessPlan) -> PlanResult:
        """Validate and execute a BusinessPlan. Returns a PlanResult.

        Steps:
        1. Audit the plan (entity names, precondition completeness)
        2. Evaluate preconditions via SQL (inside the transaction if atomic)
        3. Execute operations in order
        4. Commit or rollback
        5. Call _journal() with the outcome
        6. Return PlanResult in business language

        Never raises — all failures are captured as PlanResult(status="rejected").
        """
        raise NotImplementedError

    def _audit_plan(self, plan: BusinessPlan) -> str | None:
        """Cross-check the plan against own ontology + business rules knowledge.

        Returns None if the plan looks valid, or a rejection reason string
        if an obvious problem is found (unknown entity, missing critical precondition).

        This is the Executor's auditor role — it does not re-derive preconditions,
        only validates that the Planner's plan is internally consistent with known rules.
        """
        raise NotImplementedError

    def _evaluate_preconditions(self, plan: BusinessPlan) -> str | None:
        """Generate and run SQL checks for each Precondition in the plan.

        Returns None if all preconditions pass, or the violation message
        of the first failing precondition.

        Called inside the transaction if plan.atomic=True.
        """
        raise NotImplementedError

    def _execute_operations(self, plan: BusinessPlan) -> int:
        """Execute all Operations in order and return total rows affected.

        Called after all preconditions pass.
        Raises on SQL error — caller is responsible for rollback.
        """
        raise NotImplementedError

    def _journal(self, plan: BusinessPlan, result: PlanResult) -> None:
        """Journaling hook — NOT IMPLEMENTED. Do not implement until explicitly requested."""
        pass
