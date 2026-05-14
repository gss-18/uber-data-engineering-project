"""Floating AI chat widget rendered as a full-screen transparent iframe overlay.

The iframe injected by components.html() is repurposed into a full-viewport
transparent overlay via window.frameElement style overrides. A MutationObserver
re-applies the override if Streamlit's React reconciler resets the iframe height.
No window.parent access required.
"""

import json
import os

import streamlit.components.v1 as components

from config_utils import get_secret


_SYSTEM_PROMPT = """You are the AI co-pilot for the Uber Real-Time DE Mission Control dashboard.

The dashboard surfaces real-time analytics for an Uber-style ride-sharing platform built on:
- Azure EventHub (live ride event stream)
- Databricks Delta Lake (Bronze -> Silver -> Gold medallion architecture)
- Streamlit (this dashboard)

Available KPIs and dimensions:
- Total rides, total revenue, average fare, average surge multiplier, average rating, average distance, cancellation rate
- Rides by city, vehicle type, payment method
- Surge distribution, regional breakdown, top drivers (leaderboard)
- Live ride feed, pickup heatmap

Style: be concise (2-4 sentences), data-driven, and conversational. Use plain text,
no markdown headings. If the user asks something outside the dashboard scope, politely
steer them back. If asked for specific numbers you don't have, say so and suggest where
they can look in the dashboard."""


_CHAT_HTML = r"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html, body {
  width: 100%; height: 100%;
  background: transparent !important;
  overflow: hidden;
  pointer-events: none;
}

@keyframes chatPopIn {
  0%   { opacity: 0; transform: scale(0.4); }
  70%  { opacity: 1; transform: scale(1.08); }
  100% { opacity: 1; transform: scale(1); }
}
@keyframes chatBounce {
  0%, 80%, 100% { transform: translateY(0); opacity: 0.5; }
  40%           { transform: translateY(-5px); opacity: 1; }
}
@keyframes chatPulse {
  0%, 100% { box-shadow: 0 0 0 0 rgba(0,229,195,0.55); }
  50%      { box-shadow: 0 0 0 8px rgba(0,229,195,0); }
}
@keyframes chatMsgIn {
  from { opacity: 0; transform: translateY(8px); }
  to   { opacity: 1; transform: translateY(0); }
}

#chat-toggle {
  position: fixed; bottom: 28px; right: 28px;
  width: 48px; height: 48px; border-radius: 50%;
  background: #00e5c3;
  border: none; cursor: pointer;
  box-shadow: 0 10px 32px rgba(0,229,195,0.35), 0 0 0 1px rgba(0,229,195,0.2);
  display: flex; align-items: center; justify-content: center;
  z-index: 1;
  opacity: 0; transform: scale(0.4);
  animation: chatPopIn 0.6s 1.2s cubic-bezier(0.34,1.56,0.64,1) forwards;
  transition: transform 0.2s ease, box-shadow 0.2s ease;
  pointer-events: all;
}
#chat-toggle:hover {
  transform: scale(1.07);
  box-shadow: 0 14px 40px rgba(0,229,195,0.5);
}
#chat-toggle.is-open { background: #111116; }
#chat-toggle svg { width: 24px; height: 24px; color: #080c10; }
#chat-toggle.is-open svg { color: #00e5c3; }

#chat-panel {
  position: fixed; bottom: 96px; right: 28px;
  width: 360px; max-height: 500px; height: 500px;
  background: #0c0c10;
  border: 1px solid rgba(0,229,195,0.18);
  border-radius: 8px;
  box-shadow: 0 24px 70px rgba(0,0,0,0.55), 0 0 0 1px rgba(0,229,195,0.06);
  display: flex; flex-direction: column;
  overflow: hidden; z-index: 1;
  font-family: 'IBM Plex Mono', monospace;
  transform-origin: bottom right;
  transform: perspective(800px) rotateX(-20deg) scaleY(.82) translateY(14px);
  opacity: 0; pointer-events: none;
  transition: transform 0.5s cubic-bezier(0.2,0,0,1), opacity 0.4s cubic-bezier(0.2,0,0,1);
}
#chat-panel.is-open {
  transform: perspective(800px) rotateX(0) scaleY(1) translateY(0);
  opacity: 1; pointer-events: all;
}

#chat-header {
  display: flex; align-items: center; gap: 10px;
  padding: 14px 16px;
  border-bottom: 1px solid rgba(255,255,255,0.06);
  background: linear-gradient(180deg, rgba(0,229,195,0.06) 0%, transparent 100%);
  pointer-events: all;
}
#chat-avatar {
  width: 32px; height: 32px; border-radius: 50%;
  background: linear-gradient(135deg, #00e5c3, #00b8ff);
  display: flex; align-items: center; justify-content: center;
  color: #080c10; font-weight: 800; font-size: 12px; letter-spacing: 0.05em;
  flex-shrink: 0;
}
#chat-headertext { flex: 1; min-width: 0; }
#chat-name {
  font-size: 12px; font-weight: 700; color: #fff;
  font-family: 'IBM Plex Mono', monospace; letter-spacing: 0.02em;
}
#chat-status {
  font-size: 9px; color: rgba(0,229,195,0.85);
  letter-spacing: 0.1em; text-transform: uppercase; margin-top: 2px;
  display: flex; align-items: center; gap: 6px;
}
#chat-status::before {
  content: ''; width: 6px; height: 6px; border-radius: 50%;
  background: #00e5c3; animation: chatPulse 1.8s infinite;
}
#chat-close {
  background: transparent; border: none; cursor: pointer;
  width: 28px; height: 28px; border-radius: 6px;
  color: rgba(255,255,255,0.55); font-size: 18px; line-height: 1;
  transition: background 0.15s, color 0.15s;
  display: flex; align-items: center; justify-content: center;
  pointer-events: all;
}
#chat-close:hover { background: rgba(255,255,255,0.06); color: #fff; }

#chat-messages {
  flex: 1; overflow-y: auto; padding: 14px 16px;
  display: flex; flex-direction: column; gap: 10px;
  scroll-behavior: smooth; pointer-events: all;
}
#chat-messages::-webkit-scrollbar { width: 4px; }
#chat-messages::-webkit-scrollbar-thumb { background: rgba(0,229,195,0.25); border-radius: 2px; }

.chat-msg {
  max-width: 85%; padding: 9px 12px; border-radius: 12px;
  font-size: 12px; line-height: 1.55; word-wrap: break-word;
  animation: chatMsgIn 0.3s cubic-bezier(0.2,0,0,1);
}
.chat-msg.bot {
  align-self: flex-start;
  background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.05);
  color: rgba(255,255,255,0.92); border-top-left-radius: 4px;
}
.chat-msg.user {
  align-self: flex-end;
  background: linear-gradient(135deg, rgba(0,229,195,0.18), rgba(0,184,255,0.12));
  border: 1px solid rgba(0,229,195,0.25); color: #fff; border-top-right-radius: 4px;
}
.chat-msg.error {
  align-self: flex-start;
  background: rgba(255,68,68,0.1); border: 1px solid rgba(255,68,68,0.3);
  color: rgba(255,150,150,0.95);
}

.chat-typing {
  align-self: flex-start; display: flex; gap: 4px; padding: 11px 14px;
  background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.05);
  border-radius: 12px; border-top-left-radius: 4px;
}
.chat-typing span {
  width: 6px; height: 6px; border-radius: 50%;
  background: #00e5c3; animation: chatBounce 1.3s infinite;
}
.chat-typing span:nth-child(2) { animation-delay: 0.15s; }
.chat-typing span:nth-child(3) { animation-delay: 0.3s; }

#chat-suggestions {
  display: flex; gap: 6px; padding: 8px 16px 0;
  overflow-x: auto; scrollbar-width: none; pointer-events: all;
}
#chat-suggestions::-webkit-scrollbar { display: none; }
.chat-chip {
  flex-shrink: 0; padding: 6px 10px; border-radius: 14px;
  background: rgba(255,255,255,0.04); border: 1px solid rgba(0,229,195,0.18);
  color: rgba(255,255,255,0.78); font-size: 10px; font-family: inherit;
  letter-spacing: 0.02em; cursor: pointer; white-space: nowrap;
  transition: background 0.15s, border-color 0.15s, color 0.15s;
  pointer-events: all;
}
.chat-chip:hover { background: rgba(0,229,195,0.12); border-color: rgba(0,229,195,0.5); color: #fff; }

#chat-input-row {
  display: flex; align-items: flex-end; gap: 8px;
  padding: 12px 14px 14px;
  border-top: 1px solid rgba(255,255,255,0.06);
  pointer-events: all;
}
#chat-input {
  flex: 1; resize: none; min-height: 36px; max-height: 90px;
  background: rgba(8,12,16,0.7); border: 1px solid rgba(255,255,255,0.08);
  border-radius: 10px; color: #fff; font-family: inherit; font-size: 12px;
  padding: 9px 12px; outline: none; transition: border-color 0.15s;
  pointer-events: all;
}
#chat-input:focus { border-color: rgba(0,229,195,0.5); }
#chat-input::placeholder { color: rgba(255,255,255,0.3); }
#chat-send {
  width: 36px; height: 36px; border-radius: 10px; background: #00e5c3;
  border: none; cursor: pointer; flex-shrink: 0;
  display: flex; align-items: center; justify-content: center;
  transition: transform 0.15s, opacity 0.15s; pointer-events: all;
}
#chat-send:hover:not(:disabled) { transform: scale(1.05); }
#chat-send:disabled { opacity: 0.4; cursor: not-allowed; }
#chat-send svg { width: 16px; height: 16px; color: #080c10; }
</style>
</head>
<body>

<button id="chat-toggle" aria-label="Open AI chat">
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round">
    <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
  </svg>
</button>

<div id="chat-panel">
  <div id="chat-header">
    <div id="chat-avatar">AI</div>
    <div id="chat-headertext">
      <div id="chat-name">AI Operations Analyst</div>
      <div id="chat-status">ready &middot; gold + silver + ml</div>
    </div>
    <button id="chat-close" aria-label="Close">&times;</button>
  </div>
  <div id="chat-messages" role="log" aria-live="polite"></div>
  <div id="chat-suggestions"></div>
  <div id="chat-input-row">
    <textarea id="chat-input" rows="1" placeholder="Ask about revenue, demand, surge, forecasts…"></textarea>
    <button id="chat-send" aria-label="Send">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
        <line x1="22" y1="2" x2="11" y2="13"/>
        <polygon points="22 2 15 22 11 13 2 9 22 2"/>
      </svg>
    </button>
  </div>
</div>

<script>
(function () {
  // ── Make this iframe a full-screen transparent overlay ──────────────────
  function applyFrameStyles() {
    var fr = window.frameElement;
    if (!fr) return;
    fr.setAttribute('allowtransparency', 'true');
    fr.style.setProperty('position',       'fixed',       'important');
    fr.style.setProperty('top',            '0',           'important');
    fr.style.setProperty('left',           '0',           'important');
    fr.style.setProperty('width',          '100vw',       'important');
    fr.style.setProperty('height',         '100vh',       'important');
    fr.style.setProperty('z-index',        '9000',        'important');
    fr.style.setProperty('pointer-events', 'none',        'important');
    fr.style.setProperty('border',         'none',        'important');
    fr.style.setProperty('background',     'transparent', 'important');
    fr.style.setProperty('display',        'block',       'important');
    // Fix parent containers that may clip overflow
    var p = fr.parentElement;
    for (var i = 0; i < 8 && p; i++, p = p.parentElement) {
      p.style.setProperty('overflow', 'visible', 'important');
      if (p.tagName && p.tagName.toUpperCase() === 'BODY') break;
    }
  }
  applyFrameStyles();

  // Re-apply if Streamlit's React reconciler resets the iframe style/size
  var fr = window.frameElement;
  if (fr && fr.parentElement) {
    var guard = false;
    var mo = new MutationObserver(function () {
      if (guard) return;
      guard = true;
      applyFrameStyles();
      guard = false;
    });
    mo.observe(fr, { attributes: true, attributeFilter: ['style', 'height', 'width', 'class'] });
  }

  // ── Chat state & config ─────────────────────────────────────────────────
  var API_KEY     = "__API_KEY__";
  var MODEL       = "__MODEL__";
  var SYSTEM_PROMPT = __SYSTEM_PROMPT__;
  var FALLBACKS   = [
    "Configure GEMINI_API_KEY in .env to enable AI analysis.",
    "The pipeline is running — check metrics in the KPI grid above.",
    "I can surface demand and surge insights once the Gemini key is set."
  ];
  var _fbIdx = 0;
  var SUGGESTIONS = ["Revenue today?", "Top surge zones", "Demand forecast", "Silver-layer ops"];

  var state = { isOpen: false, isLoading: false, history: [] };

  var $toggle   = document.getElementById('chat-toggle');
  var $panel    = document.getElementById('chat-panel');
  var $messages = document.getElementById('chat-messages');
  var $sugRow   = document.getElementById('chat-suggestions');
  var $input    = document.getElementById('chat-input');
  var $send     = document.getElementById('chat-send');
  var $close    = document.getElementById('chat-close');

  // ── Helpers ─────────────────────────────────────────────────────────────
  function appendMsg(role, text) {
    var el = document.createElement('div');
    el.className = 'chat-msg ' + role;
    el.textContent = text;
    $messages.appendChild(el);
    $messages.scrollTop = $messages.scrollHeight;
  }

  function showTyping() {
    var t = document.createElement('div');
    t.className = 'chat-typing'; t.id = 'chat-typing';
    t.innerHTML = '<span></span><span></span><span></span>';
    $messages.appendChild(t);
    $messages.scrollTop = $messages.scrollHeight;
  }

  function hideTyping() {
    var t = document.getElementById('chat-typing');
    if (t) t.remove();
  }

  function setOpen(open) {
    state.isOpen = open;
    $panel.classList.toggle('is-open', open);
    $toggle.classList.toggle('is-open', open);
    if (open) setTimeout(function () { $input.focus(); }, 350);
  }

  // ── Gemini API call ──────────────────────────────────────────────────────
  async function callGemini(userText) {
    state.history.push({ role: 'user', content: userText });

    if (!API_KEY) {
      var msg = FALLBACKS[_fbIdx++ % FALLBACKS.length];
      state.history.push({ role: 'assistant', content: msg });
      return msg;
    }

    var contents = state.history.slice(-12).map(function (m) {
      return { role: m.role === 'assistant' ? 'model' : 'user', parts: [{ text: m.content }] };
    });

    var res = await fetch(
      'https://generativelanguage.googleapis.com/v1beta/models/' + MODEL +
      ':generateContent?key=' + encodeURIComponent(API_KEY),
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          system_instruction: { parts: [{ text: SYSTEM_PROMPT }] },
          contents: contents,
          generationConfig: { temperature: 0.6, maxOutputTokens: 1024 }
        })
      }
    );

    if (!res.ok) {
      var errText = 'Status ' + res.status;
      try { var j = await res.json(); if (j.error && j.error.message) errText = j.error.message; } catch(_) {}
      throw new Error(errText);
    }

    var data = await res.json();
    var cand = data.candidates && data.candidates[0];
    var text = '(no response)';
    if (cand && cand.content && cand.content.parts && cand.content.parts.length) {
      text = cand.content.parts.map(function (p) { return p.text || ''; }).join('').trim() || text;
    } else if (cand && cand.finishReason) {
      text = '(blocked: ' + cand.finishReason + ')';
    }
    state.history.push({ role: 'assistant', content: text });
    return text;
  }

  // ── Send message ─────────────────────────────────────────────────────────
  async function sendMessage(text) {
    text = (text || '').trim();
    if (!text || state.isLoading) return;
    state.isLoading = true;
    $send.disabled = true;
    $input.value = '';
    $input.style.height = 'auto';

    appendMsg('user', text);
    showTyping();

    try {
      var reply = await callGemini(text);
      hideTyping();
      appendMsg('bot', reply);
    } catch (err) {
      hideTyping();
      appendMsg('error', 'Error: ' + (err.message || 'Could not reach Gemini.'));
    } finally {
      state.isLoading = false;
      $send.disabled = false;
      $input.focus();
    }
  }

  // ── Wire up events ────────────────────────────────────────────────────────
  $toggle.addEventListener('click', function () { setOpen(!state.isOpen); });
  $close.addEventListener('click',  function () { setOpen(false); });
  $send.addEventListener('click',   function () { sendMessage($input.value); });

  $input.addEventListener('keydown', function (e) {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage($input.value); }
  });
  $input.addEventListener('input', function () {
    $input.style.height = 'auto';
    $input.style.height = Math.min($input.scrollHeight, 90) + 'px';
  });

  SUGGESTIONS.forEach(function (s) {
    var chip = document.createElement('button');
    chip.className = 'chat-chip';
    chip.textContent = s;
    chip.addEventListener('click', function () { sendMessage(s); });
    $sugRow.appendChild(chip);
  });

  // ── Greeting ──────────────────────────────────────────────────────────────
  appendMsg('bot', 'Mission intelligence active. Ask me about revenue, demand, surge, silver-layer ops, or forecasts.');

  // ── Auto-open ─────────────────────────────────────────────────────────────
  setTimeout(function () { setOpen(true); }, 1500);
})();
</script>
</body>
</html>
"""


def render_floating_chat() -> None:
    """Render the floating Gemini chat as a full-screen transparent iframe overlay."""
    api_key = (
        get_secret("GEMINI_API_KEY")
        or os.getenv("GEMINI_API_KEY", "")
        or os.getenv("GOOGLE_API_KEY", "")
        or ""
    )

    html = (
        _CHAT_HTML
        .replace("__API_KEY__", api_key.replace("\\", "\\\\").replace('"', '\\"'))
        .replace("__MODEL__", "gemini-2.5-flash")
        .replace("__SYSTEM_PROMPT__", json.dumps(_SYSTEM_PROMPT))
    )

    components.html(html, height=1, scrolling=False)
