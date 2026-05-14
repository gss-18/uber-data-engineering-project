"""AI Operations Analyst hero panel with collapsible ReAct agent chat."""

from __future__ import annotations

import streamlit as st

from react_agent_service import answer_with_react_agent, is_react_agent_configured


def render_ai_operations(data: dict) -> None:
    st.markdown(
        '<div class="ai-panel">'
        '<div class="ai-title">'
        '<div>'
        '<div class="ai-title-kicker">AI Operations Analyst</div>'
        '<div class="ai-title-main">Mission intelligence</div>'
        '</div>'
        '<div class="ai-title-sub">'
        'A routed analyst for revenue, demand, surge,<br>'
        'silver-layer operations, forecasts, and market context.'
        '</div>'
        '</div>'
        '<div class="ai-layer-strip">'
        '<div class="ai-layer-pill"><strong>Gold</strong>KPIs and executive metrics</div>'
        '<div class="ai-layer-pill"><strong>Silver</strong>Enriched ride signals</div>'
        '<div class="ai-layer-pill"><strong>ML</strong>Demand and surge forecasts</div>'
        '<div class="ai-layer-pill"><strong>Context</strong>Market reference data</div>'
        '</div>'
        '</div>',
        unsafe_allow_html=True,
    )

    label = "✕  Close Agent" if st.session_state.get("show_react_chat") else "↗  Ask ReAct Agent"
    if st.button(label, type="primary", use_container_width=True, key="ask_react_btn"):
        st.session_state.show_react_chat = not st.session_state.get("show_react_chat", False)
        st.rerun()

    if st.session_state.get("show_react_chat", False):
        _render_react_agent_chat()


def _render_react_agent_chat() -> None:
    agent_ready = is_react_agent_configured()
    if "react_agent_history" not in st.session_state:
        st.session_state.react_agent_history = []

    if not agent_ready:
        st.info("ReAct Agent is disabled until Gemini and Databricks credentials are configured.")
        return

    examples = [
        "What's the total revenue in New York?",
        "Which are the top 5 cities by revenue?",
        "Give me a silver-layer deep dive for Chicago",
        "Compare surge pricing between New York and Los Angeles",
        "Predict demand for Dallas over the next 3 hours",
        "What is the surge pressure in Las Vegas?",
        "Add market context for San Diego",
        "Show me demand metrics for Chicago",
        "What are peak surge hours?",
        "Show me the KPI dashboard",
    ]

    col_example, col_use, col_clear = st.columns([5, 1.25, 1.25])
    with col_example:
        selected = st.selectbox("Example questions", [""] + examples, key="react_agent_example")
    with col_use:
        if selected and st.button("Use Example", type="secondary", use_container_width=True):
            st.session_state.react_agent_input = selected
    with col_clear:
        if st.button("Clear Chat", type="secondary", use_container_width=True):
            st.session_state.react_agent_history = []
            st.rerun()

    chat_scroller = st.container(height=320, border=False)
    with chat_scroller:
        if not st.session_state.react_agent_history:
            st.markdown(
                '<div class="empty-state">'
                'Ask a question about revenue, demand, surge pressure, silver-layer operations, forecasts, or market context. '
                'The analyst routes to curated read-only tools and summarizes the result.'
                '</div>',
                unsafe_allow_html=True,
            )
        else:
            for msg in st.session_state.react_agent_history[-12:]:
                with st.chat_message(msg["role"]):
                    st.markdown(msg["content"])
                    if msg["role"] == "assistant" and msg.get("metadata"):
                        with st.expander("Execution metadata"):
                            st.json(msg["metadata"])

    col_input, col_ask = st.columns([5, 1.25])
    with col_input:
        question = st.text_input(
            "Ask the ReAct agent",
            placeholder="Ask about revenue, demand, surge, forecasts…",
            key="react_agent_input",
            label_visibility="collapsed",
        )
    with col_ask:
        submit = st.button("Ask Agent", type="primary", use_container_width=True)

    if submit and question.strip():
        st.session_state.react_agent_history.append({"role": "user", "content": question.strip()})
        with chat_scroller:
            with st.chat_message("user"):
                st.markdown(question.strip())
            with st.chat_message("assistant"):
                with st.spinner("Routing to curated tools…"):
                    result = answer_with_react_agent(
                        question.strip(),
                        st.session_state.react_agent_history[:-1],
                    )
                if result["ok"]:
                    st.markdown(result["answer"])
                else:
                    st.warning(result["answer"])
                if result.get("metadata"):
                    with st.expander("Execution metadata"):
                        st.json(result["metadata"])
        st.session_state.react_agent_history.append(
            {
                "role": "assistant",
                "content": result["answer"],
                "metadata": result.get("metadata", {}),
            }
        )
