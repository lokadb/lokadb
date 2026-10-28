#!/usr/bin/env python3
"""The charts of the README's performance section, drawn from the raw results in research/bench/.

    python3 bench/charts/build.py [--out docs/img/bench]

One SVG per chart and theme: <name>-light.svg and <name>-dark.svg (the README picks one with <picture> and
prefers-color-scheme). The numbers drawn are also written to research/bench/charts-data.md, so every chart has its
table. Nothing is typed in by hand: change a result directory and run again.

Which results: the runs of 2026-10-27 (the last code). The one exception is the database without lokadb (plain), which
does not depend on lokadb's code: its baseline is the run of 2026-10-26, where the same table was measured that way.
"""
import json
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from svg import Canvas, bar_panel, fmt, line_panel, xy_panel, y_axis, nice_max  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent.parent
BENCH = ROOT / "research" / "bench"
OUT = ROOT / "docs" / "img" / "bench"
TABLES = []  # (heading, header, rows)


# ---- data -----------------------------------------------------------------------------------------------------------

def rows(d):
    out = []
    for f in sorted((BENCH / d / "raw").glob("*.jsonl")):
        if f.name.startswith("resources") or f.name.startswith("writer"):
            continue
        for line in f.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("{"):
                try:
                    out.append(json.loads(line))
                except ValueError:
                    pass
    return out


def sel(rs, **want):
    out = []
    for r in rs:
        t = dict(r.get("tags", {}))
        t["op"] = r.get("op", t.get("op"))
        if all(str(t.get(k, "")) == str(v) for k, v in want.items()):
            out.append(r)
    return out


def med(rs, key):
    xs = [r[key] for r in rs if r.get(key) is not None]
    return statistics.median(xs) if xs else None


def qps(d, **want):
    return med(sel(rows(d), **want), "qps")


def p50(d, **want):
    return med(sel(rows(d), **want), "p50_ms")


def mix_qps(d, **want):
    """read+update of a mix run: per repeat the two operations added, the median over the repeats"""
    per = defaultdict(float)
    ops = errs = 0
    for r in sel(rows(d), **want):
        per[r["tags"].get("repeat", "1")] += r["qps"]
        ops += r.get("ops", 0)
        errs += r.get("errors", 0)
    return (statistics.median(per.values()) if per else None), (errs / ops if ops else 0)


def num(v, digits=1):
    """exact numbers for the tables: 29 799, 0.61, 28.4"""
    if v is None:
        return "-"
    if abs(v) >= 1000:
        return f"{round(v):,}".replace(",", " ")
    if abs(v) >= 100:
        return f"{v:.0f}"
    return f"{v:.{digits}f}".rstrip("0").rstrip(".") if digits else f"{v:.0f}"


def table(heading, header, body):
    TABLES.append((heading, header, body))


# ---- the charts -----------------------------------------------------------------------------------------------------

def theme_canvas(theme, w, h, title, desc):
    c = Canvas(theme, w, h, title, desc)
    c.text(24, 30, title, 16, "ink", "start", 600)
    return c


def chart_proxy_cost(theme):
    ops = ["read", "feed", "insert", "update"]
    after, paths = "2026-10-27-after", "2026-10-26-paths"
    by32 = [qps(after, config="fdw", **{"class": "cloud"}, clients=32, op=o) for o in ops]
    by1 = [p50(after, config="fdw", **{"class": "cloud"}, clients=1, op=o) for o in ops]
    pl32, pl1 = [], []
    for o in ops:
        if o == "feed":  # the feed against the same table with the index (organization_id, id)
            pl32.append(qps(after, config="plain", **{"class": "cloud"}, clients=32, op=o))
            pl1.append(p50(after, config="plain", **{"class": "cloud"}, clients=1, op=o))
        else:
            pl32.append(qps(paths, config="plain", clients=32, keys="cold", op=o))
            pl1.append(p50(paths, config="plain", clients=1, keys="cold", op=o))
    table("proxy-cost: organizations that stayed in the cloud, through the proxy and straight to Postgres",
          ["operation", "without lokadb ops/s (32 clients)", "through lokadb ops/s (32 clients)", "without lokadb p50 ms (1 client)", "through lokadb p50 ms (1 client)"],
          [[o, num(a), num(b), num(x, 2), num(y, 2)] for o, a, b, x, y in zip(ops, pl32, by32, pl1, by1)])
    c = theme_canvas(theme, 760, 346, "What the proxy costs organizations that stay in the cloud",
                     "Throughput at 32 clients and median latency at one client for read, feed, insert and update, with and without lokadb.")
    c.legend(24, 52, ["without lokadb", "through lokadb"])
    bar_panel(c, 70, 112, 300, 160, ops, [("without", pl32), ("lokadb", by32)], "Throughput, 32 clients", "ops/s")
    bar_panel(c, 450, 112, 270, 160, ops, [("without", pl1), ("lokadb", by1)], "Latency, 1 client", "p50 ms",
              label_fmt=lambda v: fmt(v, 2))
    c.text(24, 330, "Extended protocol, 20 organizations of 20 000 documents, keys never seen before; a feed is ORDER BY id DESC LIMIT 20 with an index (organization_id, id).", 10.5, "muted")
    return c


def chart_new_connection(theme):
    after, extra = "2026-10-27-after", "2026-10-27-extra"
    direct = qps(after, config="plain", reconnect=1)
    pgb = qps(extra, config="plain+pgbouncer-transaction")
    nopool = qps(after, config="fdw", **{"class": "cloud"}, reconnect=1)
    pool = qps(after, config="fdw+upstream_pool", **{"class": "cloud"}, reconnect=1)
    cats = ["straight to\nPostgres", "pgbouncer\n(transaction)", "lokadb,\nno pool", "lokadb, pool\n(the default)"]
    vals = [direct, pgb, nopool, pool]
    table("new-connection: a new connection for every request, 16 clients, one read",
          ["path", "requests/s"], [[n.replace("\n", " "), num(v)] for n, v in zip(cats, vals)])
    c = theme_canvas(theme, 760, 346, "A new connection for every request",
                     "Requests per second with a connection opened for each read, 16 clients: straight to Postgres, through pgbouncer, through lokadb without and with its connection cache.")
    x0, y0, w, h = 70, 104, 640, 160
    top, ticks = nice_max(max(vals))
    c.text(x0 - 40, y0 - 22, "Requests per second (read, 16 clients)", 13, "ink", "start", 600)
    y_axis(c, x0, y0, w, h, top, ticks, "req/s")
    slot = w / len(cats)
    for i, (cat, v) in enumerate(zip(cats, vals)):
        bx = x0 + slot * i + slot / 2 - 12
        yt = y0 + h - h * v / top
        c.bar(bx, yt, 24, y0 + h, c.t["s"][1] if i == 3 else c.t["s"][0])
        c.text(bx + 12, yt - 6, fmt(v), 12, "ink", "middle")
        for j, part in enumerate(cat.split("\n")):
            c.text(x0 + slot * i + slot / 2, y0 + h + 18 + 15 * j, part, 12, "ink2", "middle")
    c.text(24, 330, "The pool is on by default; pgbouncer here is our plain setup (scram, one process, pool of 32), not its best case.", 10.5, "muted")
    return c


def chart_fdw_vs_direct(theme):
    d = "2026-10-27-rtt"
    rs = sel(rows(d), shape="tx", keys="cold", clients=1)
    rtts = sorted({float(r["tags"]["rtt_ms"]) for r in rs})
    def series(op):
        return [[med([r for r in sel(rs, op=op, config=cfg) if float(r["tags"]["rtt_ms"]) == t], "p50_ms") for t in rtts] for cfg in ("fdw", "fdw+direct")]
    rd, up = series("read"), series("update")
    labels = [f"{t:.1f} ms" if t >= 1 else f"{t:.2f} ms" for t in rtts]
    table("fdw-vs-direct: the latency of one operation of a moved organization by the distance to it (RTT from the cloud), in a transaction, 1 client",
          ["RTT", "read fdw p50 ms", "read fdw+direct p50 ms", "update fdw p50 ms", "update fdw+direct p50 ms"],
          [[l, num(rd[0][i], 1), num(rd[1][i], 1), num(up[0][i], 1), num(up[1][i], 1)] for i, l in enumerate(labels)])
    c = theme_canvas(theme, 760, 366, "The price of distance for a moved organization",
                     "Median latency of a read and of an update by the round-trip time to the organization's database, for the fdw and fdw+direct profiles.")
    c.legend(24, 52, ["fdw", "fdw+direct"])
    xs = [round(t, 1) for t in rtts]
    xy_panel(c, 70, 112, 290, 160, 45, [0, 10, 20, 30, 40], [("fdw", list(zip(xs, rd[0]))), ("fdw+direct", list(zip(xs, rd[1])))], "Read (p50)", "ms", "distance to the organization, ms (RTT)", [-10, 14])
    xy_panel(c, 450, 112, 260, 160, 45, [0, 10, 20, 30, 40], [("fdw", list(zip(xs, up[0]))), ("fdw+direct", list(zip(xs, up[1])))], "Update (p50)", "ms", "distance to the organization, ms (RTT)", [-10, 14])
    far = rtts[-1]
    f_rd, d_rd, d_up = rd[0][-1], rd[1][-1], up[1][-1]
    c.text(24, 322, f"Round trips per operation (latency / distance at {far:.0f} ms): read {f_rd / far:.1f} with fdw, {d_rd / far:.1f} with fdw+direct; update {d_up / far:.1f} with fdw+direct.", 11, "ink2")
    c.text(24, 344, "One client in a transaction, keys never seen before; the organization's database 0.2 to 43 ms from the cloud.", 10.5, "muted")
    return c


def chart_feed_join(theme):
    h0, h1 = "2026-10-27-http", "2026-10-27-fixed-http"
    pa, af = "2026-10-26-paths", "2026-10-27-after"
    panels = [
        ("Django feed, moved org, fdw (16 clients)", qps(h0, app="django", config="fdw", **{"class": "moved"}, clients=16, op="feed", keys="cold"),
         qps(h1, app="django", config="fdw", **{"class": "moved"}, clients=16, op="feed", keys="cold")),
        ("Django feed, moved org, fdw+direct (16 clients)", qps(h0, app="django", config="fdw+direct", **{"class": "moved"}, clients=16, op="feed", keys="cold"),
         qps(h1, app="django", config="fdw+direct", **{"class": "moved"}, clients=16, op="feed", keys="cold")),
        ("Feed, cloud org, through lokadb (32 clients)", qps(pa, config="fdw", **{"class": "cloud"}, clients=32, op="feed", keys="cold"),
         qps(af, config="fdw", **{"class": "cloud"}, clients=32, op="feed")),
        ("Read, cloud org, through lokadb (32 clients)", qps(pa, config="fdw", **{"class": "cloud"}, clients=32, op="read", keys="cold"),
         qps(af, config="fdw", **{"class": "cloud"}, clients=32, op="read")),
    ]
    table("feed-join: before and after the fixes of 26–27 October (ops/s)",
          ["measurement", "before", "after", "times"], [[n, num(b), num(a), f"{a / b:.1f}"] for n, b, a in panels])
    c = theme_canvas(theme, 760, 470, "Before and after the fixes",
                     "Throughput of four operations before and after the fixes of 26 and 27 October: the Django feed of a moved organization, and the feed and the read of an organization that stays in the cloud.")
    c.legend(24, 52, ["before", "after"])
    pos = [(70, 122), (440, 122), (70, 312), (440, 312)]
    for (x, y), (name, b, a) in zip(pos, panels):
        bar_panel(c, x, y, 230, 90, ["before", "after"], [("before", [b, None]), ("after", [None, a])], name, "ops/s", solo=True)
    c.text(24, 458, "Before: the first run of each (26 October; the Django feed, 27 October morning). After: the code of the evening of 27 October.", 10.5, "muted")
    return c


def chart_scale_orgs(theme):
    d = "2026-10-27-orgs"
    ns = [1, 100, 1000, 3000]
    ser = []
    for op in ("read", "update", "insert"):
        ser.append((op, [qps(d, orgs=n, clients=32, op=op) for n in ns]))
    table("scale-orgs: throughput through the proxy by the number of organizations, 32 clients (ops/s)",
          ["organizations"] + [s[0] for s in ser], [[str(n)] + [num(s[1][i]) for s in ser] for i, n in enumerate(ns)])
    c = theme_canvas(theme, 760, 336, "Thousands of organizations",
                     "Throughput of reads, updates and inserts through the proxy at 32 clients with 1, 100, 1000 and 3000 organizations.")
    c.legend(24, 52, [s[0] for s in ser])
    line_panel(c, 70, 112, 640, 150, ns, ser, "Throughput, 32 clients", "ops/s", [str(n) for n in ns], label_last=True, label_dy=[20, -12, -12])
    c.text(24, 318, "Each organization is a partition; 20 000 documents each at 1–100 organizations, fewer per organization at 1000 and 3000.", 10.5, "muted")
    return c


LAG = re.compile(r"step=catch-up .*?rows=(\d+) rows_total=(\d+) .*?elapsed=([0-9hms]+)")


def secs(s):
    m = re.fullmatch(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?", s)
    h, mi, se = (int(x or 0) for x in m.groups())
    return h * 3600 + mi * 60 + se


def lag_series(log):
    pts = []
    for line in (BENCH / log).read_text(encoding="utf-8", errors="replace").splitlines():
        m = LAG.search(line)
        if m:
            pts.append((secs(m.group(3)) / 60, (int(m.group(2)) - int(m.group(1))) / 1e6))
    return pts


def pauses(log):
    t = (BENCH / log).read_text(encoding="utf-8", errors="replace")
    m = re.search(r"writes_paused=([0-9.]+)(ms|s)", t)
    return float(m.group(1)) / (1000 if m.group(2) == "ms" else 1) if m else None


def chart_pause_by_size(theme):
    sizes = [("60 MB", "rows300000-body200"), ("600 MB", "rows3000000-body200")]
    trig = [pauses(f"2026-10-27-trigger2-move/raw/move-{s}-trigger-batch10000.log") for _, s in sizes]
    logi = []
    for _, s in sizes:
        v = None
        for d in ("2026-10-27-logical8d-move", "2026-10-27-logical8c-move"):
            f = BENCH / d / "raw" / f"move-{s}-logical-batch10000.log"
            if f.exists() and "move-org done" in f.read_text(encoding="utf-8", errors="replace"):
                v = pauses(f"{d}/raw/{f.name}")
        logi.append(v)
    lag8 = lag_series("2026-10-27-move/raw/move-rows300000-body200-trigger-batch10000.log")
    lag2 = lag_series("2026-10-27-trigger2-move/raw/move-rows300000-body200-trigger-batch10000.log")
    table("pause-by-size: the pause of writes at the switch of a move, seconds", ["size", "trigger, 2 writers", "logical, 8 writers"],
          [[n, num(a, 2) if a is not None else "-", num(b, 2) if b is not None else "not measured"] for (n, _), a, b in zip(sizes, trig, logi)])
    table("pause-by-size: the lag of the trigger move (changes not yet applied, millions) by time, 60 MB", ["minute", "8 writers", "2 writers"],
          [[f"{m:.0f}", num(next((v for t, v in lag8 if t >= m), lag8[-1][1]), 2), num(next((v for t, v in lag2 if t >= m), lag2[-1][1]) if lag2 and m <= lag2[-1][0] else 0, 2)] for m in (5, 15, 30, 45, 60, 65)])
    c = theme_canvas(theme, 760, 372, "Moving an organization under write load",
                     "The pause of writes at the switch by the size of the organization, and the lag of the trigger move over time with two and with eight writers.")
    ser = [("trigger, 2 writers", trig)] + ([("logical, 8 writers", logi)] if any(v is not None for v in logi) else [])
    c.legend(24, 52, ["trigger, 2 writers", "logical, 8 writers", "trigger, 8 writers"])
    bar_panel(c, 70, 112, 250, 160, [n for n, _ in sizes], ser, "Pause of writes at the switch", "s", label_fmt=lambda v: fmt(v, 1))
    # lag over time: x in minutes, y millions of changes
    x0, y0, w, h = 440, 112, 280, 160
    tmax = max(t for t, _ in lag8)
    vmax, ticks = nice_max(max(v for _, v in lag8))
    c.text(x0 - 40, y0 - 30, "Lag of the trigger move (million changes)", 13, "ink", "start", 600)
    y_axis(c, x0, y0, w, h, vmax, ticks, "")
    for m in range(0, int(tmax) + 1, 20):
        c.text(x0 + w * m / tmax, y0 + h + 18, f"{m} min" if m == 60 else f"{m}", 11, "muted", "middle")
    for si, pts in ((0, lag2), (2, lag8)):
        p = [(x0 + w * t / tmax, y0 + h - h * v / vmax) for t, v in pts]
        if p:
            c.polyline(p, c.t["s"][si])
            c.dot(*p[-1], c.t["s"][si])
    peak = max(v for _, v in lag8)
    stop_min = max(t for t, v in lag8 if v >= peak * 0.97)
    c.text(x0 + 6, y0 + 56, f"8 writers: {peak:.1f} million behind", 11, "ink2")
    c.text(x0 + 6, y0 + 72, f"at {stop_min:.0f} min; caught up {tmax - stop_min:.0f} min", 11, "ink2")
    c.text(x0 + 6, y0 + 88, "after the writers stopped.", 11, "ink2")
    c.text(x0 + 6, y0 + h - 10, f"2 writers: lag cleared in {lag2[-1][0] * 60:.0f} s.", 11, "ink2")
    c.text(24, 310, "Eight writers add changes faster than the trigger move applies them: it caught up only after the writers stopped.", 11, "ink2")
    c.text(24, 332, "Organization 43 ms from the cloud; lag on 60 MB; 8 writers 3 300 inserts/s, 2 writers 450-680/s.", 10.5, "muted")
    c.text(24, 350, "The pause is the time writes to the organization wait while the move swaps its partitions.", 10.5, "muted")
    return c


def chart_client_ceiling(theme):
    d = "2026-10-27-clients"
    cl = [200, 500, 1000]
    ser_cloud = [("without lokadb", [mix_qps(d, config="plain", **{"class": "cloud"}, clients=n, keys="cold")[0] for n in cl]),
                 ("through lokadb", [mix_qps(d, config="fdw", **{"class": "cloud"}, clients=n, keys="cold")[0] for n in cl])]
    mv = [mix_qps(d, config="fdw", **{"class": "moved"}, clients=n, keys="cold") for n in cl]
    ser_moved = [("moved, fdw", [m[0] for m in mv])]
    table("client-ceiling: read+update throughput (ops/s) by the number of clients; refused share for moved organizations",
          ["clients", "cloud, without lokadb", "cloud, through lokadb", "moved, fdw", "moved, fdw: operations that failed"],
          [[str(n), num(ser_cloud[0][1][i]), num(ser_cloud[1][1][i]), num(mv[i][0]), f"{mv[i][1] * 100:.0f} %"] for i, n in enumerate(cl)])
    c = theme_canvas(theme, 760, 356, "How far the clients go",
                     "Read plus update throughput at 200, 500 and 1000 clients: organizations in the cloud with and without lokadb, and moved organizations through fdw.")
    c.legend(24, 52, ["without lokadb", "through lokadb"])
    line_panel(c, 70, 112, 290, 160, cl, ser_cloud, "Organizations in the cloud", "ops/s", [str(n) for n in cl])
    line_panel(c, 450, 112, 260, 160, cl, [ser_moved[0]], "Moved organizations (fdw)", "ops/s", [str(n) for n in cl])
    c.text(410, 304, f"At 1000 clients {mv[-1][1] * 100:.0f} % of the operations fail: the relay reaches", 11, "ink2")
    c.text(410, 320, "its max_streams (2048 connections through it at once).", 11, "ink2")
    c.text(24, 342, "Read+update mix, keys never seen before, 20 organizations, 4 of them moved to databases 0.2–43 ms away.", 10.5, "muted")
    return c


def chart_overload(theme):
    d = "2026-10-27-open-extra"
    rates = [17500, 20000, 22500, 25000]
    p50s, p99s, bad = [], [], []
    for r in rates:
        rs = sel(rows(d), config="fdw", rate=r)
        rd = [x for x in rs if x.get("op") == "read"]
        p50s.append(med(rd, "p50_ms"))
        p99s.append(med(rd, "p99_ms"))
        ops = sum(x.get("ops", 0) for x in rs)
        errs = sum(x.get("errors", 0) for x in rs)
        bad.append(100.0 * errs / ops if ops else 0)
    plain_p50 = med([x for x in sel(rows(d), config="plain", rate=25000) if x.get("op") == "read"], "p50_ms")
    table("overload: the open model, organizations in the cloud through the proxy (256 workers, read+update at a set rate, timeout 1 s)",
          ["offered ops/s", "p50 ms (read)", "p99 ms (read)", "timeouts %"],
          [[str(r), num(a, 1), num(b, 0), f"{c:.1f}"] for r, a, b, c in zip(rates, p50s, p99s, bad)] + [["25000 without lokadb", num(plain_p50, 2), "-", "0.0"]])
    labels = [f"{r / 1000:g}K" for r in rates]
    c = theme_canvas(theme, 760, 352, "What overload looks like",
                     "Median latency of reads and the share of timeouts through the proxy when requests arrive at a set rate of 17 500 to 25 000 per second, read plus update.")
    c.legend(24, 52, ["through lokadb", "without lokadb"])
    bar_panel(c, 70, 112, 300, 160, labels + ["25K plain"], [("lokadb", p50s + [None]), ("plain", [None] * 4 + [plain_p50])], "Median latency of a read", "ms", label_fmt=lambda v: fmt(v, 1), solo=True)
    bar_panel(c, 450, 112, 270, 160, labels, [("lokadb", bad)], "Requests that timed out", "%", label_fmt=lambda v: f"{v:.1f}")
    c.text(24, 312, "Requests arrive at the offered rate (read plus update per second) whether or not the earlier ones were answered.", 11, "ink2")
    c.text(24, 334, "256 workers, a request not started or not answered in 1 s is a timeout; the ceiling of the closed loop is 24-30K ops/s.", 10.5, "muted")
    return c


CHARTS = {
    "proxy-cost": chart_proxy_cost,
    "new-connection": chart_new_connection,
    "fdw-vs-direct": chart_fdw_vs_direct,
    "feed-join": chart_feed_join,
    "scale-orgs": chart_scale_orgs,
    "pause-by-size": chart_pause_by_size,
    "client-ceiling": chart_client_ceiling,
    "overload": chart_overload,
}


def main(argv):
    out = OUT
    if "--out" in argv:
        out = Path(argv[argv.index("--out") + 1])
    out.mkdir(parents=True, exist_ok=True)
    for name, fn in CHARTS.items():
        for theme in ("light", "dark"):
            TABLES.clear() if theme == "light" and False else None
            c = fn(theme)
            (out / f"{name}-{theme}.svg").write_text(c.svg(), encoding="utf-8", newline="\n")
        print("chart", name)
    # one set of tables (the light run's; the numbers do not depend on the theme)
    seen, md = set(), ["# Numbers behind the charts\n", "Generated by `python3 bench/charts/build.py` from the results in this directory.\n"]
    for heading, header, body in TABLES:
        if heading in seen:
            continue
        seen.add(heading)
        md.append(f"\n## {heading}\n\n| " + " | ".join(header) + " |\n|" + "---|" * len(header) + "\n" + "\n".join("| " + " | ".join(r) + " |" for r in body) + "\n")
    (BENCH / "charts-data.md").write_text("".join(md), encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main(sys.argv[1:])
