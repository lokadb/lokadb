#!/usr/bin/env python3
"""Tables and charts for the README from the raw data of research/bench/<date>-<scenario>/.

    python3 bench/report.py [RESULT_DIR ...]      default: every directory of research/bench with a meta.json
    python3 bench/report.py --out research/bench/REPORT.md

Standard library only. For each scenario it reads raw/*.jsonl (the lines of benchload, each with
"tags": scenario, config, class, op, clients, repeat, ...) and meta.json, takes the median over the
repeats of every number, and writes
    <dir>/report.md            tables of that scenario
    <dir>/<chart>.svg          charts (latency and throughput by configuration)
and one REPORT.md that joins the tables of all scenarios with the conditions of every run (commit,
servers, RTT). A run marked "trial" in meta.json is labelled so and is never meant for the README.
"""
import json
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BENCH = ROOT / "research" / "bench"
CONFIG_ORDER = ["plain", "fdw", "fdw+direct", "direct"]
COLORS = {"plain": "#3b7ea1", "fdw": "#c1553a", "fdw+direct": "#d79b2b", "direct": "#4f9d69"}


def load(d: Path):
    rows = []
    for f in sorted((d / "raw").glob("*.jsonl")):
        for line in f.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "tags" in r:
                t = r["tags"]
                # One line per operation: the tag "op" of a run says what the run was (mix, copy, cursor, connect), the
                # line's own "op" says what it measured (read and update of a mix; copy_out_query, copy_in; connect, first_query).
                t["case"] = t.get("op", "")
                t["op"] = r.get("op", t.get("op", ""))
                label = t.get("config", "")
                if t.get("shape") == "tx" and label != "direct":
                    label += " (tx)"
                if label == "lokadb-direct":  # performance review 3, P3-20: the application's set_config is a separate exchange here
                    label += " (set_config apart)"
                if t.get("keys") == "hot":
                    label += " (hot keys)"
                if t.get("app"):
                    label = t["app"] + " " + label
                t["label"] = label
                rows.append(r)
    return rows


def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def fmt(x, nd=2):
    if x is None:
        return "-"
    if x >= 1000:
        return f"{x:,.0f}".replace(",", " ")
    return f"{x:.{nd}f}".rstrip("0").rstrip(".") if nd else f"{x:.0f}"


def spread(s) -> str:
    """the median of the repeats and, with more than one, their range: 1 200 (1 150-1 260)"""
    if s["repeats"] > 1 and s["qps_min"] is not None and s["qps_min"] != s["qps_max"]:
        return f"{fmt(s['qps'], 0)} ({fmt(s['qps_min'], 0)}–{fmt(s['qps_max'], 0)})"
    return fmt(s["qps"], 0)


def group(rows, keys):
    g = defaultdict(list)
    for r in rows:
        g[tuple(r["tags"].get(k, "") for k in keys)].append(r)
    return g


def summarize(rs):
    """median over the repeats of one (config, class, op, clients)"""
    out = {k: med([r.get(k) for r in rs]) for k in ("qps", "p50_ms", "p95_ms", "p99_ms", "max_ms", "mean_ms")}
    out["errors"] = sum(r.get("errors", 0) for r in rs)
    out["ops"] = sum(r.get("ops", 0) for r in rs)
    out["repeats"] = len(rs)
    q = [r.get("qps") for r in rs if r.get("qps") is not None]
    out["qps_min"], out["qps_max"] = (min(q), max(q)) if q else (None, None)
    errs = [r.get("first_error") for r in rs if r.get("first_error")]
    out["first_error"] = errs[0] if errs else ""
    return out


def table(headers, rows):
    s = "| " + " | ".join(headers) + " |\n|" + "|".join("---" for _ in headers) + "|\n"
    for r in rows:
        s += "| " + " | ".join(str(c) for c in r) + " |\n"
    return s


def color(label: str) -> str:
    """the configuration of a label ("laravel fdw+direct (tx)", "fdw+pools") picks the colour"""
    for name in sorted(COLORS, key=len, reverse=True):
        if name in label.replace("+pools", ""):
            return COLORS[name]
    return "#888"


def bar_chart(path: Path, title: str, unit: str, cats, series, values):
    """grouped bars: cats on the x axis, one bar per series; values[(cat, series)] -> number or None"""
    W, H, L, B, T = 760, 340, 64, 56, 40
    vmax = max([v for v in values.values() if v] or [1]) * 1.12
    gw = (W - L - 16) / max(len(cats), 1)
    bw = gw * 0.8 / max(len(series), 1)
    o = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" font-family="sans-serif" font-size="12">',
         f'<rect width="{W}" height="{H}" fill="#fff"/>',
         f'<text x="{L}" y="22" font-size="14" font-weight="bold">{title} ({unit})</text>']
    for i in range(5):
        y = H - B - (H - B - T) * i / 4
        o.append(f'<line x1="{L}" x2="{W-8}" y1="{y:.1f}" y2="{y:.1f}" stroke="#ddd"/>')
        o.append(f'<text x="{L-6}" y="{y+4:.1f}" text-anchor="end">{fmt(vmax*i/4, 1)}</text>')
    for ci, c in enumerate(cats):
        x0 = L + ci * gw + gw * 0.1
        for si, s in enumerate(series):
            v = values.get((c, s))
            if v is None:
                continue
            h = (H - B - T) * v / vmax
            x = x0 + si * bw
            o.append(f'<rect x="{x:.1f}" y="{H-B-h:.1f}" width="{bw*0.92:.1f}" height="{h:.1f}" fill="{color(s)}"><title>{s} {c}: {fmt(v)}</title></rect>')
        o.append(f'<text x="{L+ci*gw+gw/2:.1f}" y="{H-B+16}" text-anchor="middle">{c}</text>')
    lx = L
    for s in series:
        o.append(f'<rect x="{lx}" y="{H-18}" width="10" height="10" fill="{color(s)}"/><text x="{lx+14}" y="{H-9}">{s}</text>')
        lx += 20 + 8 * len(s)
    o.append("</svg>")
    path.write_text("\n".join(o), encoding="utf-8")


def conditions(d: Path) -> str:
    try:
        m = json.loads((d / "meta.json").read_text(encoding="utf-8"))
    except Exception:
        return "_no meta.json: the run did not finish_\n"
    s = f"- run: {m.get('date')} ({m.get('scenario')}), lokadb `{m.get('lokadb_commit', '').strip()}`, Postgres {m.get('postgres')}\n"
    if m.get("trial"):
        s += "- **TRIAL RUN: every role on one machine; the numbers debug the scripts and are not results**\n"
    s += f"- load: warm-up {m.get('warmup')}, measured {m.get('duration')}, {m.get('repeats')} repeats (median)\n"
    for h, v in (m.get("hosts") or {}).items():
        s += f"- {h}: {v.get('cpus')} CPU, {v.get('mem_mb')} MB, disk {v.get('disk')}\n"  # a host is named by the roles on it, never by an address
    for name in ("rtt-start.json", "rtt-end.json"):
        try:
            r = json.loads((d / name).read_text(encoding="utf-8"))
            s += f"- RTT ms min/avg/max, {'start' if 'start' in name else 'end'}: " + "; ".join(f"{k} {v}" for k, v in r.items()) + "\n"
        except Exception:
            pass
    return s


def report_paths(d: Path, rows):
    """paths, http: latency and throughput by configuration, class, operation, clients"""
    g = group(rows, ["label", "class", "op", "clients"])
    summ = {k: summarize(v) for k, v in g.items()}
    md = ""
    configs = [c for c in CONFIG_ORDER if any(k[0] == c for k in summ)] + sorted({k[0] for k in summ} - set(CONFIG_ORDER))
    for cls in sorted({k[1] for k in summ}):
        for clients in sorted({k[3] for k in summ if k[1] == cls and k[3]}, key=lambda x: int(x) if x.isdigit() else 0):
            ops = sorted({k[2] for k in summ if k[1] == cls and k[3] == clients}, key=lambda o: ["read", "feed", "insert", "update", "report"].index(o) if o in ("read", "feed", "insert", "update", "report") else 9)
            tb = []
            for op in ops:
                for c in configs:
                    s = summ.get((c, cls, op, clients))
                    if s:
                        tb.append([op, c, spread(s), fmt(s["p50_ms"]), fmt(s["p95_ms"]), fmt(s["p99_ms"]), s["errors"] or "", (s["first_error"][:60] if s["errors"] else "")])
            if tb:
                md += f"\n#### organizations: {cls}, {clients} client(s)\n\n" + table(["op", "config", "req/s (min–max of repeats)", "p50 ms", "p95 ms", "p99 ms", "errors", "first error"], tb)
            for metric, label, unit in (("p50_ms", "p50 latency", "ms"), ("qps", "throughput", "req/s")):
                vals = {(op, c): (summ.get((c, cls, op, clients)) or {}).get(metric) for op in ops for c in configs}
                if any(vals.values()):
                    name = f"{d.name}-{cls}-{clients}c-{metric}.svg".replace("+", "_")
                    bar_chart(d / name, f"{label}, organizations {cls}, {clients} client(s)", unit, ops, configs, vals)
                    md += f"\n![{label} {cls} {clients}]({name})\n"
    conn = {k: summarize(v) for k, v in group([r for r in rows if r.get("op") in ("connect", "first_query")], ["label", "class", "op"]).items()}
    if conn:
        md += "\n#### a new connection\n\n" + table(["config", "class", "step", "p50 ms", "p95 ms", "p99 ms"],
                                                   [[k[0], k[1], k[2], fmt(s["p50_ms"]), fmt(s["p95_ms"]), fmt(s["p99_ms"])] for k, s in sorted(conn.items())])
    return md


def report_generic(d: Path, rows):
    """any other scenario: one table of what each line measured"""
    tags = sorted({t for r in rows for t in r["tags"] if t not in ("scenario", "repeat", "op", "case", "label")})
    g = group(rows, tags + ["op"])
    tb = []
    for k, v in sorted(g.items()):
        s = summarize(v)
        tb.append(list(k) + [spread(s), fmt(s["p50_ms"]), fmt(s["p95_ms"]), fmt(s["p99_ms"]), fmt(s["max_ms"]), s["errors"] or "", s["first_error"][:70] if s["errors"] else ""])
    return "\n" + table(tags + ["op", "ops/s (min–max of repeats)", "p50 ms", "p95 ms", "p99 ms", "max ms", "errors", "first error"], tb) if tb else ""


def report_outage(d: Path, rows):
    """outage: from the timelines, per kind of outage and class of organizations: what happened and for how long"""
    import csv
    md = "\n"
    tb = []
    for f in sorted((d / "raw").glob("outage-*.csv")):
        kind = f.stem.removeprefix("outage-")
        data = list(csv.DictReader(f.open(encoding="utf-8")))
        classes = sorted({r["class"] for r in data})
        by = {c: {int(r["second"]): r for r in data if r["class"] == c} for c in classes}
        # the outage: the longest run of seconds in which the first moved class completed nothing
        victim = next((c for c in classes if c.startswith("moved")), None)
        if not victim:
            continue
        secs = sorted(by[victim])
        run, best, best_start = 0, 0, 0
        for sec in range(secs[0], secs[-1] + 1):
            r = by[victim].get(sec)
            if r and int(r["ok"]) == 0 and int(r["errors"]) > 0:
                run += 1
                if run > best:
                    best, best_start = run, sec - run + 1
            else:
                run = 0
        end = best_start + best
        for c in classes:
            def tot(lo, hi, key):
                return sum(int(by[c][s][key]) for s in range(lo, hi) if s in by[c])
            before_ok, before_err = tot(0, best_start, "ok"), tot(0, best_start, "errors")
            during_ok, during_err = tot(best_start, end, "ok"), tot(best_start, end, "errors")
            after_ok, after_err = tot(end, 10**6, "ok"), tot(end, 10**6, "errors")
            lat = [float(by[c][s]["mean_ms"]) for s in range(0, best_start) if s in by[c] and int(by[c][s]["ok"])]
            lat2 = [float(by[c][s]["mean_ms"]) for s in range(best_start, end) if s in by[c] and int(by[c][s]["ok"])]
            el = [float(by[c][s].get("error_mean_ms") or 0) for s in range(best_start, end) if s in by[c] and int(by[c][s]["errors"])]
            tb.append([kind, c, f"{best_start}-{end} s" if c == victim else "", before_err, f"{during_ok} / {during_err}", after_err,
                       fmt(med(lat)), fmt(med(lat2)), fmt(med(el)) if c == victim else ""])
    if tb:
        md += table(["outage", "class", "seen as down", "errors before", "during: ok / errors", "errors after", "mean ms before", "mean ms during", "time to the error ms"], tb)
    return md


def main(argv):
    out = BENCH / "REPORT.md"
    dirs = []
    i = 0
    while i < len(argv):
        if argv[i] == "--out":
            out = Path(argv[i + 1]); i += 2
        else:
            dirs.append(Path(argv[i])); i += 1
    if not dirs:
        dirs = sorted(p for p in BENCH.glob("*-*") if (p / "raw").is_dir())
    total = "# Measurements\n\nGenerated by `python3 bench/report.py` from the raw data in each directory; how they were taken: `METHOD.md`.\n"
    for d in dirs:
        rows = load(d)
        scen = (rows[0]["tags"].get("scenario") if rows else d.name.split("-", 3)[-1]) or d.name
        body = f"\n## {d.name}\n\n" + conditions(d)
        if scen in ("paths", "http"):
            body += report_paths(d, rows)
        elif scen == "outage":
            body += report_outage(d, rows) + report_generic(d, rows)
        else:
            body += report_generic(d, rows)
        check = d / "data-check.txt"
        if check.exists():  # bench/verify-data.py: the rows by organization against what benchload counted
            body += "\n### data check\n\n" + "".join(f"- {l}\n" for l in check.read_text(encoding="utf-8").splitlines() if l.strip())
        (d / "report.md").write_text(body, encoding="utf-8")
        total += body
    out.write_text(total, encoding="utf-8")
    print(f"{len(dirs)} scenario(s) -> {out}")


if __name__ == "__main__":
    main(sys.argv[1:])
