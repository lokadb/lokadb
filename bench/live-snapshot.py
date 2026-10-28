#!/usr/bin/env python3
"""The live view of a run for the page that shows it: one JSON document from what the scenarios have logged so far.

    python3 bench/live-snapshot.py INVENTORY_NAME [--state running|idle|done] [--out FILE]

INVENTORY_NAME is the name of the inventory file without its extension (bench/state/<name>/). Reads live.jsonl (one line per
run: the scenario, the tags, the lines benchload printed) and the newest resource samples of the run in
research/bench/<date>-<scenario>/raw/resources.jsonl, and writes
  {state, scenario, variant, updated_at, links: {"load-proxy": {ops, p50, p99, err, what}, "proxy-cloud": ..., "org1": ...},
   nodes: {cloud|proxy|load|org1..: {cpu, mem_gb}}}
Roles only: no address, no place. A flow that is not measured is not in `links`.
"""
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BENCH = ROOT / "research" / "bench"


def last_lines(path: Path, n=1):
    try:
        return [json.loads(l) for l in path.read_text(encoding="utf-8", errors="replace").splitlines() if l.strip().startswith("{")][-n:]
    except OSError:
        return []


def gb(text: str) -> float | None:
    m = re.match(r"\s*([\d.]+)\s*([KMGT]i?B)", text or "")
    if not m:
        return None
    v, unit = float(m.group(1)), m.group(2)[0]
    return round(v * {"K": 1e-6, "M": 1e-3, "G": 1, "T": 1000}[unit], 2)


def variant(tags: dict) -> str:
    parts = []
    for k in ("app", "config", "class", "protocol", "shape", "keys", "orgs", "meta_cache", "sessions", "agents", "default_rows", "rtt_ms"):
        if tags.get(k):
            parts.append(f"{k}={tags[k]}" if k in ("orgs", "meta_cache", "sessions", "agents", "default_rows", "rtt_ms") else tags[k])
    if tags.get("op"):
        parts.append(tags["op"])
    if tags.get("clients"):
        parts.append(f"{tags['clients']} clients")
    return ", ".join(parts)


def moved_orgs(st: Path, scenario: str) -> list[str]:
    """The organizations a moved class's flow goes to: all of the stand's, one in the scenarios that move one."""
    try:
        orgs = (st / "orgs").read_text().split()
    except OSError:
        return []
    if scenario == "agents" and orgs:
        return [os.environ.get("AGENTS_ORG", orgs[-1])]
    if scenario == "move" and os.environ.get("MOVE_ORG"):
        return [os.environ["MOVE_ORG"]]
    return orgs


def build_doc(r, orgs, resources, state="running", updated_at=None):
    """The live view of one run `r` (a live.jsonl line: scenario, tags, lines): the links with their numbers, the moved
    organizations' links without numbers (`active`), the hosts' load from `resources` (the lines of a resources.jsonl)."""
    doc = {"state": state, "scenario": "", "variant": "", "updated_at": updated_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "links": {}, "active": [], "nodes": {}}
    if r:
        tags, lines = r.get("tags", {}), r.get("lines", [])
        doc["scenario"], doc["variant"] = r.get("scenario", ""), variant(tags)
        ok = [x for x in lines if x.get("ops")]
        if ok:
            ops = round(sum(x.get("qps", 0) for x in ok), 1)
            busiest = max(ok, key=lambda x: x.get("qps", 0))
            p50, p99 = busiest.get("p50_ms", 0), busiest.get("p99_ms", 0)
            err = sum(x.get("errors", 0) for x in lines)
            link = {"ops": ops, "p50": p50, "p99": p99, "err": err, "what": variant(tags)}
            cfg = tags.get("config", "")
            plain = cfg == "plain" or cfg.startswith("plain")
            if plain:
                doc["links"]["load-cloud"] = link
            else:
                doc["links"]["load-proxy"] = link
                # the cloud carries the flow when the class stays there, and in fdw / fdw+direct, where it reaches the moved rows
                if (tags.get("class") != "moved" or cfg in ("fdw", "fdw+direct")) and not tags.get("org"):
                    doc["links"]["proxy-cloud"] = dict(link)
            if tags.get("org"):
                doc["links"][f"org{tags['org']}"] = dict(link, what=variant(tags))
            elif not plain and tags.get("class") == "moved":
                # traffic of moved organizations without a measured share of each: animated, no numbers
                doc["active"] = [f"org{o}" for o in orgs(r.get("scenario", ""))]
            if plain:
                doc["variant"] = f"{variant(tags)}: {ops:g} op/s, p99 {p99:g} ms"
            elif tags.get("class") == "moved":
                # one flow of a class of organizations is not any one organization's: no org links, the class's own numbers in the variant
                doc["variant"] = f"moved orgs, {variant(tags)}: {ops:g} op/s, p99 {p99:g} ms"
    for rec in resources:
        role = rec.get("role", "")
        if role == "host":
            for part in str(rec.get("host", "")).split("+"):
                if part in ("cloud", "proxy", "load") or re.fullmatch(r"org\d+", part):
                    doc["nodes"].setdefault(part, {})["cpu"] = rec.get("cpu_busy_pct")
        elif role in ("cloud",) or re.fullmatch(r"org\d+", role):
            n = doc["nodes"].setdefault(role, {})
            n["mem_gb"] = gb(str(rec.get("MemUsage", "")).split("/")[0])
            n.setdefault("cpu", None)
        elif role in ("proxy", "relay") and isinstance(rec.get("process"), dict):
            n = doc["nodes"].setdefault("proxy", {})
            if role == "proxy":
                n["mem_gb"] = round(rec["process"].get("rss_mb", 0) / 1024, 2)
    return doc


def main(argv):
    if not argv:
        print(__doc__)
        return 2
    name = argv[0]
    state = "running"
    out = None
    if "--state" in argv:
        state = argv[argv.index("--state") + 1]
    if "--out" in argv:
        out = argv[argv.index("--out") + 1]
    st = ROOT / "bench" / "state" / name
    runs = last_lines(st / "live.jsonl", 12)
    r = runs[-1] if runs else None
    scen = r.get("scenario", "") if r else ""
    # the newest resource sample of the scenario's results
    res_files = sorted(BENCH.glob(f"*-{scen}/raw/resources.jsonl"), key=lambda p: p.stat().st_mtime) if scen else []
    doc = build_doc(r, lambda scenario: moved_orgs(st, scenario), last_lines(res_files[-1], 60) if res_files else [], state)
    text = json.dumps(doc, ensure_ascii=False, indent=1)
    if out:
        Path(out).write_text(text, encoding="utf-8")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
