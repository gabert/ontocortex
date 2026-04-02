"""Streamlit UI for the domain agent.

Run with:
    streamlit run streamlit_app.py
"""

import pandas as pd
import streamlit as st

from agentcore.agent import DomainAgent
from agentcore.config import load_config
from agentcore.domain import list_domains, load_domain
from agentcore.setup import database_ready, db_config_for_domain, install_domain

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Domain Agent",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Session state ─────────────────────────────────────────────────────────────

def _init_state() -> None:
    defaults = {
        "agent": None,
        "domain_name": None,
        "messages": [],        # {role, content} — display history
        "last_data": None,     # last SELECT result rows for data pane
        "last_query_log": [],  # last turn's query log
        "hood_log": [],        # cumulative query log for under-the-hood pane
        "turn_count": 0,
        "status": "No domain loaded.",
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val

_init_state()

# ── Config (cached — load once per session) ───────────────────────────────────

@st.cache_resource
def get_config():
    return load_config()

config = get_config()
domains_dir = config.domains_dir

# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_domain(domain_key: str) -> None:
    domain = load_domain(domain_key, domains_dir)
    db_cfg = db_config_for_domain(config, domain)
    if not database_ready(db_cfg, domain.database_name):
        st.toast(f"Database not found — installing {domain.name}…")
        db_cfg = install_domain(config, domain)
    config.database = db_cfg
    st.session_state.agent = DomainAgent(config, domain, verbose=False)
    st.session_state.domain_name = domain.name
    st.session_state.messages = []
    st.session_state.last_data = None
    st.session_state.last_query_log = []
    st.session_state.hood_log = []
    st.session_state.turn_count = 0
    st.session_state.status = f"Active: {domain.name}"


def _reset_domain(domain_key: str) -> None:
    domain = load_domain(domain_key, domains_dir)
    db_cfg = install_domain(config, domain)
    config.database = db_cfg
    st.session_state.agent = DomainAgent(config, domain, verbose=False)
    st.session_state.domain_name = domain.name
    st.session_state.messages = []
    st.session_state.last_data = None
    st.session_state.last_query_log = []
    st.session_state.hood_log = []
    st.session_state.turn_count = 0
    st.session_state.status = f"Active: {domain.name} (fresh DB)"


def _extract_last_data(query_log: list[dict]) -> list[dict] | None:
    """Return the last non-empty SELECT result from the query log."""
    for entry in reversed(query_log):
        if entry.get("type") == "sql" and not entry.get("is_write"):
            results = entry.get("results")
            if isinstance(results, list) and results:
                return results
    return None


_HIDDEN_COLUMNS = {"created_at"}
_HIDDEN_SUFFIXES = ("_id",)


def _format_for_customer(rows: list[dict]) -> pd.DataFrame:
    """Convert raw DB rows into a customer-friendly DataFrame.

    - Drops internal columns (created_at, *_id FK columns)
    - Renames snake_case → Title Case
    - Formats dates and booleans
    """
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Drop hidden columns
    drop = [
        c for c in df.columns
        if c in _HIDDEN_COLUMNS
        or (c.endswith(_HIDDEN_SUFFIXES) and c != df.columns[0])  # keep PK
    ]
    df = df.drop(columns=drop, errors="ignore")

    # Format values
    for col in df.columns:
        # Booleans → Yes / No
        if df[col].dtype == bool or df[col].apply(lambda v: isinstance(v, bool)).any():
            df[col] = df[col].map({True: "Yes", False: "No", None: ""})
        else:
            # Dates and datetimes → readable string
            try:
                converted = pd.to_datetime(df[col], errors="raise")
                if converted.dt.time.eq(pd.Timestamp("00:00:00").time()).all():
                    df[col] = converted.apply(lambda v: f"{v.day} {v.strftime('%b %Y')}")
                else:
                    df[col] = converted.apply(lambda v: f"{v.day} {v.strftime('%b %Y %H:%M')}")
            except Exception:
                pass

    # Rename columns: snake_case → Title Case, strip leading table prefix
    def _label(col: str) -> str:
        # strip common prefixes like "dependent_", "policy_" etc.
        parts = col.split("_")
        return " ".join(p.capitalize() for p in parts)

    df = df.rename(columns={c: _label(c) for c in df.columns})

    return df


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("Domain Agent")
    st.caption("Ontology-driven LLM agent framework")
    st.divider()

    available = list_domains(domains_dir)
    selected = st.selectbox("Domain", available)

    st.button(
        "Load domain",
        use_container_width=True,
        on_click=_load_domain,
        args=(selected,),
        help="Switch to this domain. Sets up the database if it doesn't exist yet.",
    )
    st.button(
        "Reset database",
        use_container_width=True,
        on_click=_reset_domain,
        args=(selected,),
        disabled=st.session_state.agent is None,
        help="Drop and recreate the database with fresh seed data.",
    )
    st.button(
        "New conversation",
        use_container_width=True,
        on_click=lambda: (
            st.session_state.agent.reset(),
            st.session_state.messages.clear(),
            st.session_state.hood_log.clear(),
            setattr(st.session_state, "turn_count", 0),
            setattr(st.session_state, "last_data", None),
        ),
        disabled=st.session_state.agent is None,
        help="Clear conversation history, keep the current domain and database.",
    )

    st.divider()
    if st.session_state.agent is None:
        st.warning(st.session_state.status)
    else:
        st.success(st.session_state.status)

# ── Main layout: chat | data ──────────────────────────────────────────────────

chat_col, data_col = st.columns([3, 2], gap="large")

# ── Chat pane ─────────────────────────────────────────────────────────────────

with chat_col:
    st.subheader("Conversation")

    # Fixed-height scrollable message history — only this box scrolls
    chat_container = st.container(height=500)
    with chat_container:
        for msg in st.session_state.messages:
            with st.chat_message(msg["role"]):
                st.write(msg["content"])

    if st.session_state.agent is None:
        st.info("Select a domain and click **Load domain** to start.")
    else:
        if prompt := st.chat_input("Ask something…"):
            st.session_state.messages.append({"role": "user", "content": prompt})

            # Render user message immediately — don't wait for rerun
            with chat_container:
                with st.chat_message("user"):
                    st.write(prompt)

            with st.spinner("Thinking…"):
                try:
                    response = st.session_state.agent.chat(prompt)
                    st.session_state.messages.append({"role": "assistant", "content": response})

                    log = st.session_state.agent.last_query_log
                    st.session_state.last_query_log = log

                    new_data = _extract_last_data(log)
                    if new_data is not None:
                        st.session_state.last_data = new_data

                    if log:
                        st.session_state.turn_count += 1
                        st.session_state.hood_log.append({
                            "type": "turn",
                            "label": f"Turn {st.session_state.turn_count}: {prompt[:60]}{'…' if len(prompt) > 60 else ''}",
                        })
                        st.session_state.hood_log.extend(log)

                except Exception as e:
                    agent = st.session_state.agent
                    if agent.messages and agent.messages[-1]["role"] == "user":
                        agent.messages.pop()
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": f"Something went wrong — {e}. Please try again.",
                    })

            st.rerun()

# ── Data pane ─────────────────────────────────────────────────────────────────

with data_col:
    st.subheader("Data")
    data_container = st.container(height=500)
    with data_container:
        if st.session_state.last_data:
            df = _format_for_customer(st.session_state.last_data)
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.caption("Query results will appear here.")

# ── Under the hood ────────────────────────────────────────────────────────────

st.subheader("Under the hood")
hood_container = st.container(height=280)
with hood_container:
    if not st.session_state.hood_log:
        st.caption("Database and tool activity will appear here.")
    else:
        # Split into turns and display latest turn first
        turns = []
        current_turn = []
        for entry in st.session_state.hood_log:
            if entry.get("type") == "turn":
                if current_turn:
                    turns.append(current_turn)
                current_turn = [entry]
            else:
                current_turn.append(entry)
        if current_turn:
            turns.append(current_turn)

        for turn in reversed(turns):
            for entry in turn:
                entry_type = entry.get("type")

                # ── Turn separator ────────────────────────────────────────────
                if entry_type == "turn":
                    st.markdown(f"---\n**{entry['label']}**")
                    continue

                # ── Tool call (non-SQL) ───────────────────────────────────────
                if entry_type == "tool":
                    st.markdown(f"**:violet[TOOL]** `{entry['name']}`")
                    st.caption(entry.get("result", ""))

                # ── SQL query ─────────────────────────────────────────────────
                elif entry_type == "sql":
                    is_write = entry.get("is_write", False)
                    result   = entry.get("results")
                    query    = entry.get("query", "")

                    # Validation errors (pre-flight blocked)
                    if isinstance(result, list) and result and isinstance(result[0], dict) and result[0].get("error") == "validation":
                        st.markdown("**:red[VALIDATION BLOCKED]**")
                        st.code(query, language="sql")
                        st.json(result)
                    elif is_write:
                        st.markdown("**:orange[WRITE]**")
                        st.code(query, language="sql")
                        if isinstance(result, dict):
                            if result.get("error") == "constraint_violation":
                                st.error(f"Constraint violation: {result}")
                            else:
                                st.caption(f"{result.get('rows_affected', 0)} row(s) affected")
                    else:
                        st.markdown("**:blue[READ]**")
                        st.code(query, language="sql")
                    if isinstance(result, list):
                        if result:
                            st.dataframe(pd.DataFrame(result), use_container_width=True, hide_index=True)
                        else:
                            st.caption("No rows returned.")
                    elif isinstance(result, dict) and "error" in result:
                        st.error(result)
