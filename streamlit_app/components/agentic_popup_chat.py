"""Agentic AI popup chat — ReAct agent with Databricks tools, triggered via a floating FAB button.

Trigger mechanism: a visually-hidden Streamlit checkbox is injected into the page.
The FAB's click handler does a native DOM .click() on that checkbox element, which
React picks up immediately (no synthetic-event tricks needed), causing a Streamlit
rerun with the new checkbox value, which opens the @st.dialog.
"""

import streamlit as st
import streamlit.components.v1 as _components

from react_agent_service import answer_with_react_agent, is_react_agent_configured


# Unique label used to locate the hidden checkbox in the DOM.
_TRIGGER_LABEL = "⚡​agent"  # ⚡<zero-width-space>agent — visually minimal, unique


_FAB_INJECTOR = r"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body>
<script>
(function () {
  var FAB_ID   = 'uber-agent-fab';
  var STYLE_ID = 'uber-agent-fab-styles';
  var LABEL    = '⚡​agent';   /* must match _TRIGGER_LABEL in Python */
  var pd;
  try { pd = window.parent.document; }
  catch (e) { return; }

  /* idempotent */
  if (pd.getElementById(FAB_ID)) return;

  if (!pd.getElementById(STYLE_ID)) {
    var sty = pd.createElement('style');
    sty.id = STYLE_ID;
    sty.textContent = `
      @keyframes ua-fab-pop {
        0%   { opacity:0; transform:scale(.5) translateY(8px) }
        70%  { opacity:1; transform:scale(1.07) translateY(-2px) }
        100% { opacity:1; transform:scale(1) }
      }
      #uber-agent-fab {
        position: fixed; bottom: 90px; right: 28px;
        width: 52px; height: 52px; border-radius: 50%;
        background: linear-gradient(135deg, #7c3aed, #6366f1);
        border: none; cursor: pointer; color: #fff;
        display: grid; place-items: center;
        box-shadow: 0 0 28px rgba(124,58,237,.4), 0 8px 24px rgba(0,0,0,.32);
        animation: ua-fab-pop .55s 1.05s cubic-bezier(.34,1.56,.64,1) both;
        transition: transform .18s ease, box-shadow .18s ease;
        z-index: 99998;
        font-family: inherit;
      }
      #uber-agent-fab:hover {
        transform: scale(1.09);
        box-shadow: 0 0 44px rgba(124,58,237,.6), 0 10px 32px rgba(0,0,0,.4);
      }
      #uber-agent-fab svg { width: 22px; height: 22px; pointer-events: none; }
      #ua-tooltip {
        position: fixed; bottom: 100px; right: 88px;
        background: rgba(12,12,16,.92); border: 1px solid rgba(124,58,237,.3);
        color: rgba(216,228,240,.8); font-size: .58rem; letter-spacing: .08em;
        padding: 5px 10px; border-radius: 6px; pointer-events: none;
        opacity: 0; transition: opacity .18s; white-space: nowrap;
        font-family: 'IBM Plex Mono', ui-monospace, monospace; z-index: 99998;
      }
      #uber-agent-fab:hover + #ua-tooltip { opacity: 1; }
    `;
    pd.head.appendChild(sty);
  }

  var fab = pd.createElement('button');
  fab.id = FAB_ID;
  fab.setAttribute('aria-label', 'Open AI Agent');
  fab.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"
    stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
    <path d="M12 2L2 7l10 5 10-5-10-5z"/>
    <path d="M2 17l10 5 10-5"/>
    <path d="M2 12l10 5 10-5"/>
  </svg>`;

  var tip = pd.createElement('div');
  tip.id = 'ua-tooltip';
  tip.textContent = 'AI Agent';

  pd.body.appendChild(fab);
  pd.body.appendChild(tip);

  fab.addEventListener('click', function () {
    /* Find the hidden Streamlit checkbox by its aria-label and native-click it.
       A native .click() is picked up by React immediately and causes a Streamlit
       rerun — no synthetic event tricks needed. */
    var chk = pd.querySelector('input[type="checkbox"][aria-label="' + LABEL + '"]');
    if (chk) {
      chk.click();
    } else {
      console.warn('[uber-agent-fab] trigger checkbox not found in DOM');
    }
  });
})();
</script>
</body></html>
"""

# CSS injected once to visually hide the trigger checkbox while keeping it in the DOM.
_HIDE_CHECKBOX_CSS = (
    "<style>"
    "[data-testid='stCheckbox']:has(input[aria-label='⚡​agent'])"
    "{ position:fixed!important; top:-9999px!important; left:-9999px!important;"
    " width:1px!important; height:1px!important; overflow:hidden!important; }"
    "</style>"
)


@st.dialog("AI Operations Analyst", width="large")
def _agent_dialog() -> None:
    agent_ready = is_react_agent_configured()

    st.markdown(
        '<div style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:12px;">'
        + "".join(
            f'<div style="font-size:.65rem;font-weight:700;letter-spacing:.1em;'
            f'text-transform:uppercase;color:#a78bfa;padding:4px 10px;'
            f'border:1px solid rgba(124,58,237,.28);border-radius:4px;">'
            f'<strong>{t[0]}</strong>&nbsp;&nbsp;{t[1]}</div>'
            for t in [
                ("Gold", "KPIs &amp; executive metrics"),
                ("Silver", "Enriched ride signals"),
                ("ML", "Demand &amp; surge forecasts"),
                ("Context", "Market reference data"),
            ]
        )
        + "</div>",
        unsafe_allow_html=True,
    )

    if not agent_ready:
        st.info("Configure GEMINI_API_KEY and Databricks credentials to enable the ReAct agent.")
        return

    if "react_agent_history" not in st.session_state:
        st.session_state.react_agent_history = []

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

    col_ex, col_use, col_clear = st.columns([5, 1.2, 1.2])
    with col_ex:
        selected = st.selectbox(
            "Example questions", [""] + examples,
            key="agent_popup_example", label_visibility="collapsed",
        )
    with col_use:
        use_clicked = st.button(
            "Use Example", type="secondary",
            use_container_width=True, key="agent_popup_use",
        )
    with col_clear:
        if st.button(
            "Clear Chat", type="secondary",
            use_container_width=True, key="agent_popup_clear",
        ):
            st.session_state.react_agent_history = []
            st.rerun()

    if use_clicked and selected:
        st.session_state.agent_popup_prefill = selected

    chat_area = st.container(height=380, border=False)
    with chat_area:
        if not st.session_state.react_agent_history:
            st.markdown(
                '<div style="text-align:center;padding:48px 24px;'
                'color:rgba(216,228,240,.35);font-size:.75rem;">'
                "Ask about revenue, demand, surge pressure, silver-layer operations,"
                "<br>forecasts, or market context."
                "</div>",
                unsafe_allow_html=True,
            )
        else:
            for msg in st.session_state.react_agent_history[-14:]:
                with st.chat_message(msg["role"]):
                    st.markdown(msg["content"])
                    if msg["role"] == "assistant" and msg.get("metadata"):
                        with st.expander("Execution details"):
                            st.json(msg["metadata"])

    prefill = st.session_state.pop("agent_popup_prefill", "")
    question = st.chat_input(
        placeholder="Ask about revenue, demand, surge, forecasts…",
        key="agent_popup_input",
    )
    if not question and prefill:
        question = prefill

    if question and question.strip():
        q = question.strip()
        st.session_state.react_agent_history.append({"role": "user", "content": q})
        with chat_area:
            with st.chat_message("user"):
                st.markdown(q)
            with st.chat_message("assistant"):
                with st.spinner("Routing to curated tools…"):
                    result = answer_with_react_agent(
                        q,
                        st.session_state.react_agent_history[:-1],
                    )
                if result["ok"]:
                    st.markdown(result["answer"])
                else:
                    st.warning(result["answer"])
                if result.get("metadata"):
                    with st.expander("Execution details"):
                        st.json(result["metadata"])
        st.session_state.react_agent_history.append(
            {
                "role": "assistant",
                "content": result["answer"],
                "metadata": result.get("metadata", {}),
            }
        )
        st.rerun()


def _on_agent_trigger_change() -> None:
    """on_change is the one place Streamlit lets us reset the widget's own key."""
    if st.session_state.get("agent_chat_trigger"):
        st.session_state.agent_chat_trigger = False
        st.session_state._open_agent_dialog = True


def render_agentic_popup_chat() -> None:
    # 1. Inject the floating purple FAB into the parent document
    _components.html(_FAB_INJECTOR, height=0)

    # 2. CSS to keep the checkbox off-screen while it remains in the DOM
    st.markdown(_HIDE_CHECKBOX_CSS, unsafe_allow_html=True)

    # 3. Hidden checkbox — FAB JS calls .click() on this; on_change captures the
    #    edge and immediately resets the widget so the next click works again.
    st.checkbox(
        _TRIGGER_LABEL,
        key="agent_chat_trigger",
        label_visibility="collapsed",   # collapsed > hidden — no reserved space
        on_change=_on_agent_trigger_change,
    )

    if st.session_state.pop("_open_agent_dialog", False):
        try:
            _agent_dialog()
        except AttributeError:
            st.info("Upgrade Streamlit to ≥ 1.36 to use the AI Agent popup.")