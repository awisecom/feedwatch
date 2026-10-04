"""A small, dependency-free SVG line chart for `feedwatch compare --svg`.

Static output for a README: legend plus direct end labels for identity, thin
2px lines, hairline grid, and light/dark colours chosen by the viewer's
colour scheme. Text uses text colours, never the series colours.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from xml.sax.saxutils import escape

# Categorical slots 1 and 2, validated for colour-vision deficiency in both modes.
LIGHT = {
    "s1": "#2a78d6",
    "s2": "#eb6834",
    "surface": "#fcfcfb",
    "ink": "#0b0b0b",
    "ink2": "#52514e",
    "muted": "#898781",
    "grid": "#e1e0d9",
    "axis": "#c3c2b7",
}
DARK = {
    "s1": "#3987e5",
    "s2": "#d95926",
    "surface": "#1a1a19",
    "ink": "#ffffff",
    "ink2": "#c3c2b7",
    "muted": "#898781",
    "grid": "#2c2c2a",
    "axis": "#383835",
}


@dataclass(slots=True)
class Series:
    name: str  # legend text
    label: str  # short end label
    points: list[tuple[float, float | None]]  # (x, y); None breaks the line
    slot: int  # 1 or 2


def _nice_step(span: float, target_ticks: int = 5) -> float:
    raw = span / max(1, target_ticks)
    mag = 10 ** math.floor(math.log10(raw)) if raw > 0 else 1
    for m in (1, 2, 2.5, 5, 10):
        if raw <= m * mag:
            return m * mag
    return 10 * mag


def _fmt(v: float) -> str:
    return f"{v:g}"


def line_chart(
    series: list[Series],
    title: str,
    subtitle: str,
    x_label: str,
    threshold: tuple[float, str] | None = None,
    width: int = 880,
    height: int = 360,
) -> str:
    left, right, top, bottom = 56, 150, 92, 44
    pw, ph = width - left - right, height - top - bottom
    xs = [x for s in series for x, _ in s.points]
    ys = [y for s in series for _, y in s.points if y is not None]
    x_max = max(xs) if xs else 1.0
    y_step = _nice_step(max(ys) if ys else 1.0)
    y_max = y_step * math.ceil((max(ys) if ys else 1.0) / y_step)
    x_step = _nice_step(x_max, 6)

    def sx(x: float) -> float:
        return left + pw * (x / x_max if x_max else 0)

    def sy(y: float) -> float:
        return top + ph * (1 - y / y_max)

    o: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
        f'role="img" aria-label="{escape(title)}">',
        "<style>",
        _css(LIGHT, ":root"),
        "@media (prefers-color-scheme: dark) {",
        _css(DARK, ":root"),
        "}",
        "text { font-family: system-ui, -apple-system, 'Segoe UI', sans-serif; fill: var(--ink2); font-size: 12px; }",
        ".title { fill: var(--ink); font-size: 16px; font-weight: 600; }",
        ".tick { fill: var(--muted); font-size: 11px; font-variant-numeric: tabular-nums; }",
        ".grid { stroke: var(--grid); stroke-width: 1; }",
        ".axis { stroke: var(--axis); stroke-width: 1; }",
        ".line { fill: none; stroke-width: 2; stroke-linejoin: round; stroke-linecap: round; }",
        ".s1 { stroke: var(--s1); } .s2 { stroke: var(--s2); }",
        ".d1 { fill: var(--s1); } .d2 { fill: var(--s2); }",
        ".dot { stroke: var(--surface); stroke-width: 2; }",
        ".thr { stroke: var(--muted); stroke-width: 1; }",
        ".bg { fill: var(--surface); }",
        "</style>",
        f'<rect class="bg" width="{width}" height="{height}" rx="8"/>',
        f'<text class="title" x="{left}" y="28">{escape(title)}</text>',
        f'<text x="{left}" y="48">{escape(subtitle)}</text>',
    ]

    # legend
    lx: float = left
    for s in series:
        o.append(f'<line class="line s{s.slot}" x1="{lx}" y1="70" x2="{lx + 18}" y2="70"/>')
        o.append(f'<text x="{lx + 24}" y="74">{escape(s.name)}</text>')
        lx += 24 + 7.2 * len(s.name) + 28

    # grid and y ticks
    v = 0.0
    while v <= y_max + 1e-9:
        y = sy(v)
        o.append(f'<line class="grid" x1="{left}" y1="{y:.1f}" x2="{left + pw}" y2="{y:.1f}"/>')
        o.append(f'<text class="tick" x="{left - 8}" y="{y + 4:.1f}" text-anchor="end">{_fmt(v)} s</text>')
        v += y_step

    # x ticks
    v = 0.0
    while v <= x_max + 1e-9:
        x = sx(v)
        o.append(f'<text class="tick" x="{x:.1f}" y="{top + ph + 18}" text-anchor="middle">{_fmt(v)}</text>')
        v += x_step
    o.append(f'<text class="tick" x="{left + pw}" y="{top + ph + 36}" text-anchor="end">{escape(x_label)}</text>')
    o.append(f'<line class="axis" x1="{left}" y1="{top + ph}" x2="{left + pw}" y2="{top + ph}"/>')

    if threshold is not None and threshold[0] <= y_max:
        ty = sy(threshold[0])
        o.append(f'<line class="thr" x1="{left}" y1="{ty:.1f}" x2="{left + pw}" y2="{ty:.1f}"/>')
        o.append(f'<text class="tick" x="{left + 6}" y="{ty - 6:.1f}">{escape(threshold[1])}</text>')

    # lines, end dots, end labels (nudged apart only if they would overlap)
    ends: list[tuple[float, float, Series, float]] = []
    for s in series:
        d: list[str] = []
        pen_up = True
        last: tuple[float, float] | None = None
        for px, py in s.points:
            if py is None:
                pen_up = True
                continue
            d.append(f"{'M' if pen_up else 'L'}{sx(px):.1f},{sy(py):.1f}")
            pen_up = False
            last = (px, py)
        if d:
            o.append(f'<path class="line s{s.slot}" d="{" ".join(d)}"/>')
        if last is not None:
            ends.append((sx(last[0]), sy(last[1]), s, last[1]))
    ends.sort(key=lambda e: e[1])
    label_y: list[float] = []
    for _, ey, _, _ in ends:
        label_y.append(max(ey, label_y[-1] + 16) if label_y else ey)
    for (ex, ey, es, value), ly in zip(ends, label_y, strict=True):
        o.append(f'<circle class="dot d{es.slot}" cx="{ex:.1f}" cy="{ey:.1f}" r="4"/>')
        o.append(f'<text x="{ex + 10:.1f}" y="{ly + 4:.1f}">{escape(es.label)} {value:.1f} s</text>')

    o.append("</svg>")
    return "\n".join(o) + "\n"


def _css(c: dict[str, str], scope: str) -> str:
    return scope + " { " + " ".join(f"--{k}: {v};" for k, v in c.items()) + " }"
