"""Data contracts for the three-agent pipeline.

Pure dataclasses — no logic, zero imports from agentcore.
Imported by all three agents and by pipeline.py.
"""

from dataclasses import dataclass, field


@dataclass
class Operation:
    """A single business operation within a plan."""

    type: str                            # read | write | create | delete
    entity: str                          # ontology entity name (never a table name)
    lookup: dict = field(default_factory=dict)      # {id: ...} or {field: value}
    fields: list[str] = field(default_factory=list) # which fields to return (reads)
    effect: dict = field(default_factory=dict)      # write delta: {balance: {decrement: 500}}
    data: dict = field(default_factory=dict)        # full record payload (creates)


@dataclass
class Precondition:
    """A business rule that must hold before any operation executes."""

    rule: str        # natural language statement of the rule
    violation: str   # message returned to the user if the check fails


@dataclass
class BusinessPlan:
    """The full plan emitted by the Planner and consumed by the Executor."""

    intent: str                                      # plain-English summary of what the user wants
    atomic: bool                                     # true → wrap all operations in one transaction
    preconditions: list[Precondition] = field(default_factory=list)
    operations: list[Operation] = field(default_factory=list)


@dataclass
class TaskHandoff:
    """Distilled intent passed from ConversationAgent to PlannerAgent.

    Contains only what the Planner needs — never raw conversation history.
    """

    task: str                                        # plain-English description of the task
    known_entities: dict = field(default_factory=dict)  # entity name → id/value already known


@dataclass
class PlanResult:
    """Outcome returned from ExecutorAgent up through the pipeline.

    Always expressed in business language — never exposes SQL errors.
    """

    status: str          # executed | rejected
    reason: str          # human-readable explanation
    rows_affected: int = 0
