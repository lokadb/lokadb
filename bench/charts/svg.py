"""A small SVG drawing kit for the benchmark charts (no dependencies).

Two themes (light and dark) from one set of roles: the colors are the validated categorical slots of the design
system's palette (blue, orange, aqua; `scripts/validate_palette.js` passes both modes), the ink and gridlines are
its chrome tokens. Marks follow the mark specs: bars at most 24 px thick with a 4 px rounded data end and a flat
base, lines 2 px with 8 px markers ringed in the surface color, hairline solid gridlines, text in ink tokens (never
in the series color), a legend for two or more series.
"""
from xml.sax.saxutils import escape

THEMES = {
    "light": {
        "surface": "#fcfcfb", "ink": "#0b0b0b", "ink2": "#52514e", "muted": "#6d6b66", "grid": "#e1e0d9", "axis": "#c3c2b7",
        "s": ["#2a78d6", "#eb6834", "#1baf7a"], "bad": "#d03b3b",
    },
    "dark": {
        "surface": "#1a1a19", "ink": "#f0efec", "ink2": "#c3c2b7", "muted": "#a3a199", "grid": "#2c2c2a", "axis": "#383835",
        "s": ["#3987e5", "#d95926", "#199e70"], "bad": "#ec835a",
    },
}
FONT = 'system-ui, -apple-system, "Segoe UI", sans-serif'


def fmt(v, digits=1):
    """compact numbers: 950, 1.2K, 28K, 1.5M"""
    a = abs(v)
    if a >= 1e6:
        return f"{v / 1e6:.{digits}f}".rstrip("0").rstrip(".") + "M"
    if a >= 1e4:
        return f"{v / 1e3:.0f}K"
    if a >= 1e3:
        return f"{v / 1e3:.{digits}f}".rstrip("0").rstrip(".") + "K"
    if a >= 100:
        return f"{v:.0f}"
    if a >= 10:
        return f"{v:.0f}" if abs(v - round(v)) < 0.05 else f"{v:.1f}"
    if a >= 1:
        return f"{v:.1f}".rstrip("0").rstrip(".")
    return f"{v:.2f}".rstrip("0").rstrip(".")


def nice_max(v):
    """a round axis maximum above v and its ticks: steps of 1, 2, 2.5 or 5 times a power of ten, at most five intervals"""
    import math
    if v <= 0:
        return 1, [0, 1]
    e = 10 ** math.floor(math.log10(v / 5))
    for m in (1, 2, 2.5, 5, 10, 20):
        step = m * e
        n = math.ceil(v * 1.04 / step)
        if n <= 5:
            return n * step, [step * k for k in range(n + 1)]
    return v, [0, v]


class Canvas:
    def __init__(self, theme, width, height, title, desc):
        self.t = THEMES[theme]
        self.w, self.h = width, height
        self.parts = []
        self.title, self.desc = title, desc

    def add(self, s):
        self.parts.append(s)

    def text(self, x, y, s, size=12, fill="ink2", anchor="start", weight=400):
        self.add(f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" font-weight="{weight}" text-anchor="{anchor}" '
                 f'fill="{self.t.get(fill, fill)}">{escape(str(s))}</text>')

    def line(self, x1, y1, x2, y2, color="grid", width=1):
        self.add(f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{self.t.get(color, color)}" stroke-width="{width}"/>')

    def bar(self, x, y_top, w, y_base, color):
        """a vertical bar: 4 px rounded data end, flat base"""
        h = y_base - y_top
        r = min(4, h, w / 2)
        if h <= 0.5:
            return
        self.add(f'<path d="M{x:.1f},{y_base:.1f} L{x:.1f},{y_top + r:.1f} Q{x:.1f},{y_top:.1f} {x + r:.1f},{y_top:.1f} '
                 f'L{x + w - r:.1f},{y_top:.1f} Q{x + w:.1f},{y_top:.1f} {x + w:.1f},{y_top + r:.1f} L{x + w:.1f},{y_base:.1f} Z" fill="{color}"/>')

    def dot(self, x, y, color):
        self.add(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{color}" stroke="{self.t["surface"]}" stroke-width="2"/>')

    def polyline(self, pts, color):
        d = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
        self.add(f'<polyline points="{d}" fill="none" stroke="{color}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>')

    def legend(self, x, y, names):
        for i, n in enumerate(names):
            self.add(f'<rect x="{x:.1f}" y="{y - 9:.1f}" width="12" height="12" rx="3" fill="{self.t["s"][i]}"/>')
            self.text(x + 18, y + 1, n, 12, "ink2")
            x += 18 + 7 * len(n) + 22

    def svg(self):
        t = self.t
        head = (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.w} {self.h}" width="{self.w}" height="{self.h}" '
                f'role="img" aria-labelledby="t d" font-family=\'{FONT}\'><title id="t">{escape(self.title)}</title>'
                f'<desc id="d">{escape(self.desc)}</desc><rect width="{self.w}" height="{self.h}" rx="10" fill="{t["surface"]}"/>')
        return head + "".join(self.parts) + "</svg>\n"


def y_axis(c, x0, y0, w, h, ymax, ticks, unit=""):
    for v in ticks:
        y = y0 + h - h * v / ymax
        c.line(x0, y, x0 + w, y, "grid" if v else "axis")
        c.text(x0 - 8, y + 4, fmt(v), 11, "muted", "end")
    if unit:
        c.text(x0 - 8, y0 - 12, unit, 11, "muted", "end")


def bar_panel(c, x0, y0, w, h, cats, series, title, unit="", ymax=None, label_fmt=None, note=None, solo=False):
    """grouped vertical bars: series = [(name, [values])], values aligned with cats; None = no bar.
    solo: each category has one bar (the series are the categories' colors): the bar sits in the middle of its slot"""
    top = max((v for _, vs in series for v in vs if v is not None), default=1)
    ymax, ticks = nice_max(top) if ymax is None else (ymax, [ymax * i / 4 for i in range(5)])
    c.text(x0 - 40, y0 - 30, title, 13, "ink", "start", 600)
    y_axis(c, x0, y0, w, h, ymax, ticks, unit)
    slot = w / len(cats)
    n = 1 if solo else len(series)
    bw = min(24, (slot - 14) / n - 2)
    for ci, cat in enumerate(cats):
        gx = x0 + slot * ci + (slot - (bw * n + 2 * (n - 1))) / 2
        for si, (_, vs) in enumerate(series):
            v = vs[ci]
            if v is None:
                continue
            bx = gx + (0 if solo else si * (bw + 2))
            yt = y0 + h - h * v / ymax
            c.bar(bx, yt, bw, y0 + h, c.t["s"][si])
            c.text(bx + bw / 2, yt - 5, (label_fmt or fmt)(v), 11, "ink", "middle")
        c.text(x0 + slot * ci + slot / 2, y0 + h + 18, cat, 12, "ink2", "middle")
    if note:
        c.text(x0, y0 + h + 38, note, 11, "muted")


def line_panel(c, x0, y0, w, h, xs, series, title, unit="", xlabels=None, label_last=True, ymax=None, label_dy=None):
    """lines over categorical x positions (evenly spaced); series = [(name, [values])]"""
    top = max((v for _, vs in series for v in vs if v is not None), default=1)
    ymax, ticks = nice_max(top) if ymax is None else (ymax, [ymax * i / 4 for i in range(5)])
    c.text(x0 - 40, y0 - 30, title, 13, "ink", "start", 600)
    y_axis(c, x0, y0, w, h, ymax, ticks, unit)
    n = len(xs)
    px = lambda i: x0 + (w - 40) * (i / (n - 1) if n > 1 else 0.5) + 20
    for i, lab in enumerate(xlabels or xs):
        c.text(px(i), y0 + h + 18, lab, 12, "ink2", "middle")
    for si, (name, vs) in enumerate(series):
        pts = [(px(i), y0 + h - h * v / ymax) for i, v in enumerate(vs) if v is not None]
        c.polyline(pts, c.t["s"][si])
        for (x, y), v in zip(pts, [v for v in vs if v is not None]):
            c.dot(x, y, c.t["s"][si])
        if label_last:
            x, y = pts[-1]
            c.text(x, y + (label_dy[si] if label_dy else -12), fmt(vs[-1]), 11, "ink", "middle")


def xy_panel(c, x0, y0, w, h, xmax, xticks, series, title, unit="", xunit="ms", label_dy=None):
    """lines over a numeric x (true scale); series = [(name, [(x, y)])]"""
    top = max(y for _, pts in series for _, y in pts)
    ymax, ticks = nice_max(top)
    c.text(x0 - 40, y0 - 30, title, 13, "ink", "start", 600)
    y_axis(c, x0, y0, w, h, ymax, ticks, unit)
    px = lambda x: x0 + 10 + (w - 40) * x / xmax
    for t in xticks:
        c.text(px(t), y0 + h + 18, f"{t:g}", 11, "muted", "middle")
    c.text(x0 + w / 2, y0 + h + 36, xunit, 11, "muted", "middle")
    for si, (_, pts) in enumerate(series):
        p = [(px(x), y0 + h - h * y / ymax) for x, y in pts]
        c.polyline(p, c.t["s"][si])
        for x, y in p:
            c.dot(x, y, c.t["s"][si])
        x, y = p[-1]
        c.text(x + 10, y + (label_dy[si] if label_dy else 4), fmt(pts[-1][1]), 11, "ink", "start")
