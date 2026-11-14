"""Canvas candlestick chart that keeps the whole window and paints only the viewport."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit.components.v1 as components


def build_chart_payload(
    candles: pd.DataFrame,
    *,
    ticker: str,
    timeframe: str,
    lines: Optional[List[Dict[str, Any]]] = None,
    markers: Optional[List[Dict[str, Any]]] = None,
    show_volume: bool = True,
    oscillator: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Compact series for the browser. Every input bar is kept."""
    frame = candles.reset_index(drop=True)
    bars = [
        [
            str(label),
            round(float(op), 4),
            round(float(hi), 4),
            round(float(lo), 4),
            round(float(cl), 4),
            round(float(vol), 2),
        ]
        for label, op, hi, lo, cl, vol in zip(
            frame["time_label"],
            frame["open"],
            frame["high"],
            frame["low"],
            frame["close"],
            frame["volume"],
        )
    ]
    packed_lines = []
    for line in lines or []:
        values = []
        for value in line["values"]:
            if value is None or pd.isna(value):
                values.append(None)
            else:
                values.append(round(float(value), 4))
        packed_lines.append({
            "name": line["name"],
            "color": line["color"],
            "values": values,
        })
    return {
        "ticker": ticker,
        "timeframe": timeframe,
        "bars": bars,
        "lines": packed_lines,
        "markers": markers or [],
        "showVolume": bool(show_volume),
        "oscillator": oscillator,
    }


def render_market_chart(payload: Dict[str, Any], *, height: int = 720) -> None:
    """Draw the payload on a canvas. Zoom and pan stay in the browser."""
    encoded = json.dumps(payload, separators=(",", ":")).replace("<", "\\u003c")
    components.html(_CHART_HTML.replace("__PAYLOAD__", encoded), height=height, scrolling=False)


_CHART_HTML = r"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<style>
  html, body { margin: 0; height: 100%; background: #0e1117; color: #cbd5e1; font-family: sans-serif; }
  #bar { display: flex; gap: 8px; align-items: center; height: 28px; font-size: 12px; padding: 0 8px; }
  button { background: #1e293b; color: #e2e8f0; border: 1px solid #334155; border-radius: 4px; padding: 2px 8px; cursor: pointer; }
  #readout { margin-left: auto; color: #94a3b8; white-space: nowrap; overflow: hidden; }
  canvas { display: block; width: 100%; height: calc(100% - 28px); }
</style>
</head>
<body>
<div id="bar">
  <button id="fit" type="button">Show all</button>
  <button id="latest" type="button">Latest</button>
  <span id="readout"></span>
</div>
<canvas id="c"></canvas>
<script>
const DATA = __PAYLOAD__;
const bars = DATA.bars;
const n = bars.length;
const canvas = document.getElementById("c");
const ctx = canvas.getContext("2d");
const readout = document.getElementById("readout");
const padL = 58, padR = 12, padT = 8, padB = 22;
let view0 = 0, view1 = n;
let drag = null;

function resize() {
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth;
  const h = canvas.clientHeight;
  canvas.width = Math.floor(w * dpr);
  canvas.height = Math.floor(h * dpr);
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  draw();
}

function panes() {
  const h = canvas.clientHeight;
  const plotH = h - padT - padB;
  const parts = [0.72];
  if (DATA.showVolume) parts.push(0.14);
  if (DATA.oscillator) parts.push(0.14);
  const sum = parts.reduce((a, b) => a + b, 0);
  let y = padT;
  const out = [];
  parts.forEach((p) => {
    const ph = plotH * (p / sum);
    out.push({ y, h: ph });
    y += ph;
  });
  return out;
}

function indexAt(x, plotW) {
  const span = Math.max(1, view1 - view0);
  const t = (x - padL) / plotW;
  return Math.min(n - 1, Math.max(0, Math.floor(view0 + t * span)));
}

function draw() {
  const w = canvas.clientWidth, h = canvas.clientHeight;
  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = "#0e1117";
  ctx.fillRect(0, 0, w, h);
  if (!n) return;
  const plotW = Math.max(1, w - padL - padR);
  const layout = panes();
  const price = layout[0];
  const volPane = DATA.showVolume ? layout[1] : null;
  const oscPane = DATA.oscillator ? layout[layout.length - 1] : null;
  const span = Math.max(1, view1 - view0);
  let hi = -Infinity, lo = Infinity, vmax = 0;
  const step = span / plotW;
  const zoomed = span <= plotW;
  function colRange(x) {
    if (zoomed) return null;
    const a = Math.min(n - 1, Math.floor(view0 + x * step));
    const b = Math.min(n, Math.max(a + 1, Math.floor(view0 + (x + 1) * step)));
    return [a, b];
  }
  if (zoomed) {
    for (let i = view0; i < view1; i++) {
      hi = Math.max(hi, bars[i][2]);
      lo = Math.min(lo, bars[i][3]);
      vmax = Math.max(vmax, bars[i][5]);
    }
  } else {
    for (let x = 0; x < plotW; x++) {
      const [a, b] = colRange(x);
      let bucket = 0;
      for (let i = a; i < b; i++) {
        hi = Math.max(hi, bars[i][2]);
        lo = Math.min(lo, bars[i][3]);
        bucket += bars[i][5];
      }
      vmax = Math.max(vmax, bucket);
    }
  }
  for (const m of DATA.markers) {
    if (m.index >= view0 && m.index < view1) {
      hi = Math.max(hi, m.price);
      lo = Math.min(lo, m.price);
    }
  }
  if (!isFinite(hi) || hi === lo) { hi = (hi || 1) * 1.01; lo = (lo || 0) * 0.99; }
  const yPad = (hi - lo) * 0.04;
  hi += yPad; lo -= yPad;
  const yOf = (priceY, pane) => pane.y + (hi - priceY) / (hi - lo) * pane.h;
  const xOf = (i) => padL + ((i - view0 + 0.5) / span) * plotW;

  ctx.strokeStyle = "#334155";
  ctx.lineWidth = 1;
  ctx.font = "11px sans-serif";
  ctx.fillStyle = "#94a3b8";
  for (let g = 0; g < 5; g++) {
    const val = lo + (hi - lo) * (g / 4);
    const y = yOf(val, price);
    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(w - padR, y);
    ctx.stroke();
    ctx.fillText(val.toFixed(2), 4, y + 4);
  }

  function candleAt(i, x, width) {
    const o = bars[i][1], hgh = bars[i][2], l = bars[i][3], c = bars[i][4];
    const up = c >= o;
    ctx.strokeStyle = ctx.fillStyle = up ? "#10B981" : "#EF4444";
    ctx.beginPath();
    ctx.moveTo(x, yOf(hgh, price));
    ctx.lineTo(x, yOf(l, price));
    ctx.stroke();
    const top = yOf(Math.max(o, c), price);
    const bot = yOf(Math.min(o, c), price);
    ctx.fillRect(x - width / 2, top, Math.max(1, width), Math.max(1, bot - top));
  }

  if (zoomed) {
    const width = Math.max(1, plotW / span * 0.7);
    for (let i = view0; i < view1; i++) candleAt(i, xOf(i), width);
  } else {
    for (let x = 0; x < plotW; x++) {
      const [a, b] = colRange(x);
      let hgh = -Infinity, l = Infinity, vol = 0;
      for (let i = a; i < b; i++) {
        hgh = Math.max(hgh, bars[i][2]);
        l = Math.min(l, bars[i][3]);
        vol += bars[i][5];
      }
      const o = bars[a][1], c = bars[b - 1][4];
      const up = c >= o;
      ctx.strokeStyle = ctx.fillStyle = up ? "#10B981" : "#EF4444";
      const px = padL + x + 0.5;
      ctx.beginPath();
      ctx.moveTo(px, yOf(hgh, price));
      ctx.lineTo(px, yOf(l, price));
      ctx.stroke();
      const top = yOf(Math.max(o, c), price);
      const bot = yOf(Math.min(o, c), price);
      ctx.fillRect(px - 0.5, top, 1.5, Math.max(1, bot - top));
    }
  }

  for (const line of DATA.lines) {
    ctx.strokeStyle = line.color;
    ctx.lineWidth = 1.4;
    ctx.beginPath();
    let started = false;
    const samples = zoomed ? Math.ceil(span) : plotW;
    for (let s = 0; s < samples; s++) {
      const i = zoomed ? view0 + s : Math.min(n - 1, Math.floor(view0 + (s + 0.5) * step));
      const v = line.values[i];
      if (v == null) { started = false; continue; }
      const x = zoomed ? xOf(i) : padL + s;
      const y = yOf(v, price);
      if (!started) { ctx.moveTo(x, y); started = true; }
      else ctx.lineTo(x, y);
    }
    ctx.stroke();
  }

  for (const m of DATA.markers) {
    if (m.index < view0 || m.index >= view1) continue;
    const x = xOf(m.index);
    const y = yOf(m.price, price);
    ctx.fillStyle = m.color;
    ctx.beginPath();
    if (m.side === "up") {
      ctx.moveTo(x, y - 7); ctx.lineTo(x - 5, y + 4); ctx.lineTo(x + 5, y + 4);
    } else {
      ctx.moveTo(x, y + 7); ctx.lineTo(x - 5, y - 4); ctx.lineTo(x + 5, y - 4);
    }
    ctx.closePath();
    ctx.fill();
  }

  if (volPane) {
    ctx.fillStyle = "#94a3b8";
    ctx.fillText("Vol", 4, volPane.y + 12);
    const samples = zoomed ? Math.ceil(span) : plotW;
    for (let s = 0; s < samples; s++) {
      let vol = 0, up = true;
      if (zoomed) {
        const i = view0 + s;
        vol = bars[i][5];
        up = bars[i][4] >= bars[i][1];
      } else {
        const [a, b] = colRange(s);
        for (let i = a; i < b; i++) vol += bars[i][5];
        up = bars[b - 1][4] >= bars[a][1];
      }
      const x = zoomed ? xOf(view0 + s) : padL + s;
      const bh = vmax > 0 ? (vol / vmax) * (volPane.h - 4) : 0;
      ctx.fillStyle = up ? "rgba(16,185,129,0.75)" : "rgba(239,68,68,0.75)";
      const bw = zoomed ? Math.max(1, plotW / span * 0.7) : 1.5;
      ctx.fillRect(x - bw / 2, volPane.y + volPane.h - bh, bw, bh);
    }
  }

  if (oscPane && DATA.oscillator) {
    const osc = DATA.oscillator;
    let ohi = -Infinity, olo = Infinity;
    for (const series of osc.series) {
      for (let i = view0; i < view1; i++) {
        const v = series.values[i];
        if (v == null) continue;
        ohi = Math.max(ohi, v); olo = Math.min(olo, v);
      }
    }
    if (!isFinite(ohi) || ohi === olo) { ohi = 100; olo = 0; }
    const op = (ohi - olo) * 0.08 || 1;
    ohi += op; olo -= op;
    const oy = (v) => oscPane.y + (ohi - v) / (ohi - olo) * oscPane.h;
    ctx.fillStyle = "#94a3b8";
    ctx.fillText(osc.name, 4, oscPane.y + 12);
    for (const series of osc.series) {
      ctx.strokeStyle = series.color;
      ctx.beginPath();
      let started = false;
      for (let i = view0; i < view1; i += Math.max(1, Math.floor(span / plotW))) {
        const v = series.values[i];
        if (v == null) { started = false; continue; }
        const x = xOf(i), y = oy(v);
        if (!started) { ctx.moveTo(x, y); started = true; } else ctx.lineTo(x, y);
      }
      ctx.stroke();
    }
  }

  ctx.fillStyle = "#94a3b8";
  const ticks = 6;
  for (let t = 0; t < ticks; t++) {
    const i = Math.min(n - 1, Math.floor(view0 + (span - 1) * (t / (ticks - 1))));
    ctx.fillText(bars[i][0], xOf(i) - 28, h - 6);
  }
  readout.textContent = n.toLocaleString() + " bars loaded · drag to pan · scroll to zoom";
}

canvas.addEventListener("wheel", (ev) => {
  ev.preventDefault();
  const rect = canvas.getBoundingClientRect();
  const plotW = Math.max(1, canvas.clientWidth - padL - padR);
  const focus = indexAt(ev.clientX - rect.left, plotW);
  const span = Math.max(8, view1 - view0);
  const next = Math.max(8, Math.min(n, Math.round(span * (ev.deltaY > 0 ? 1.2 : 0.8))));
  const left = (focus - view0) / span;
  view0 = Math.round(focus - left * next);
  view1 = view0 + next;
  if (view0 < 0) { view1 -= view0; view0 = 0; }
  if (view1 > n) { view0 -= view1 - n; view1 = n; }
  view0 = Math.max(0, view0);
  draw();
}, { passive: false });

canvas.addEventListener("mousedown", (ev) => { drag = { x: ev.clientX, view0, view1 }; });
window.addEventListener("mouseup", () => { drag = null; });
window.addEventListener("mousemove", (ev) => {
  const rect = canvas.getBoundingClientRect();
  const plotW = Math.max(1, canvas.clientWidth - padL - padR);
  if (drag) {
    const span = drag.view1 - drag.view0;
    const shift = Math.round((drag.x - ev.clientX) / plotW * span);
    view0 = Math.max(0, Math.min(n - span, drag.view0 + shift));
    view1 = view0 + span;
    draw();
    return;
  }
  if (ev.clientX < rect.left || ev.clientX > rect.right) return;
  const i = indexAt(ev.clientX - rect.left, plotW);
  const b = bars[i];
  readout.textContent = b[0] + "  O " + b[1] + "  H " + b[2] + "  L " + b[3] + "  C " + b[4] + "  V " + Math.round(b[5]).toLocaleString();
});

document.getElementById("fit").onclick = () => { view0 = 0; view1 = n; draw(); };
document.getElementById("latest").onclick = () => {
  const span = Math.min(n, 240);
  view0 = n - span; view1 = n; draw();
};
window.addEventListener("resize", resize);
resize();
</script>
</body>
</html>
"""
