"""Three-agent pipeline components.

Import order matters for the anti-spaghetti rules:
- base.py                          — no agentcore imports
- conversation, planner, executor  — import plan.py and base.py only, never each other
- pipeline.py                      — the only file that imports all three agents
"""
