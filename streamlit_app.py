"""Streamlit UI for the domain agent.

Run with:
    streamlit run streamlit_app.py
"""

import pandas as pd
import streamlit as st

from agentcore.pipeline import AgentPipeline
from agentcore.config import load_config
from agentcore.domain import list_domains, load_domain
from agentcore.domain.install import database_ready, db_config_for_domain, install_domain

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
    st.session_state.agent = AgentPipeline(config, domain, verbose=False)
    st.session_state.domain_name = domain.name
    st.session_state.messages = []
    st.session_state.last_query_log = []
    st.session_state.hood_log = []
    st.session_state.turn_count = 0
    st.session_state.status = f"Active: {domain.name}"


def _reset_domain(domain_key: str) -> None:
    domain = load_domain(domain_key, domains_dir)
    db_cfg = install_domain(config, domain)
    config.database = db_cfg
    st.session_state.agent = AgentPipeline(config, domain, verbose=False)
    st.session_state.domain_name = domain.name
    st.session_state.messages = []
    st.session_state.last_query_log = []
    st.session_state.hood_log = []
    st.session_state.turn_count = 0
    st.session_state.status = f"Active: {domain.name} (fresh DB)"


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
        ),
        disabled=st.session_state.agent is None,
        help="Clear conversation history, keep the current domain and database.",
    )

    st.divider()
    if st.session_state.agent is None:
        st.warning(st.session_state.status)
    else:
        st.success(st.session_state.status)

# ── Main layout: conversation | under the hood ────────────────────────────────

chat_col, hood_col = st.columns([1, 1], gap="large")

# ── Chat pane ─────────────────────────────────────────────────────────────────

with chat_col:
    st.subheader("Conversation")

    # Fixed-height scrollable message history — only this box scrolls
    chat_container = st.container(height=600)
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

                    if log:
                        st.session_state.turn_count += 1
                        st.session_state.hood_log.append({
                            "type": "turn",
                            "label": f"Turn {st.session_state.turn_count}: {prompt[:60]}{'…' if len(prompt) > 60 else ''}",
                        })
                        st.session_state.hood_log.extend(log)

                except Exception as e:
                    # Pipeline already rolled back conversation history via
                    # snapshot — nothing to clean up here.
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": f"Something went wrong — {e}. Please try again.",
                    })

            st.rerun()

# ── Under the hood ────────────────────────────────────────────────────────────

with hood_col:
    st.subheader("Under the hood")
    hood_container = st.container(height=600)
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

                    # ── Turn separator ────────────────────────────────────────
                    if entry_type == "turn":
                        st.markdown(f"---\n**{entry['label']}**")
                        continue

                    # ── SIF operation ─────────────────────────────────────────
                    if entry_type == "sif":
                        is_write = entry.get("is_write", False)
                        result   = entry.get("result")
                        query    = entry.get("sql", "")

                        if is_write:
                            st.markdown("**:orange[WRITE]**")
                        else:
                            st.markdown("**:blue[READ]**")
                        st.code(query, language="sql")

                        if isinstance(result, list):
                            if result:
                                st.dataframe(pd.DataFrame(result), use_container_width=True, hide_index=True)
                            else:
                                st.caption("No rows returned.")
                        elif isinstance(result, dict):
                            if "error" in result:
                                st.error(result)
                            else:
                                st.caption(f"{result.get('rows_affected', 0)} row(s) affected")

                    # ── Action ────────────────────────────────────────────────
                    elif entry_type == "action":
                        st.markdown(f"**:violet[ACTION]** `{entry.get('action', '')}`")
                        st.caption(str(entry.get("result", "")))
