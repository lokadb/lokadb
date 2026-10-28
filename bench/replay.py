#!/usr/bin/env python3
"""Replays a measured pass on the stand page, sped up, from the results in research/bench/.

    python3 bench/replay.py plan [--out DIR] [--frames N] [--scenarios paths,clients,rtt,move]
    python3 bench/replay.py show DIR            (what the frames hold)

`plan` turns the raw results into FRAMES (default 36). A frame is a set of writes to the page's database, in the shape
the page reads (the same documents a live run writes):

    bench/live          the run on screen: the links with their numbers, `active` (the moved organizations' links
                        without numbers), the hosts' load, the variant
    scenarios/<id>      status pending | running | done, started_at, finished_at, headline
    log/<id>            a line of the journal (info | ok)

The documents are written to DIR (default bench/replay-out): index.json lists the frames in order, each with its writes
(op, collection, doc id, file with the document's fields; a write to a document that exists is pinned with its version
by whoever applies it, a journal line is new). Frame 0 resets the scenarios to pending; the last frame ends the pass.

The database of a published page is written through the Claude app's artifact tool (there is no other writer), so the
frames are applied by a Claude Code session: "play the replay of the pass" - one batch of writes per frame, at the pace
asked for (about 5 s a frame: 36 frames are about 3 minutes). Nothing is written by this script itself.

What a frame shows is real: every run of every scenario comes from its raw results (the tags and lines of
`benchload`, the resources sampled in the run); only the clock is made up (the runs are spread over the frames of their
scenario in order, the journal's times follow the frames). No address, no place: hosts are by role. The moves are
drawn from the `move-org` log of one logical move under writes (the rows copied, then the changes caught up).
"""
import argparse
import importlib.util
import json
import re
import statistics
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BENCH = ROOT / "research" / "bench"
SOURCES = {  # scenario id on the page -> the results directory
    "paths": "2026-10-26-paths",
    "http": "2026-10-27-fixed-http",
    "overhead": "2026-10-26-overhead",
    "clients": "2026-10-27-clients",
    "sessions": "2026-10-27-sessions",
    "rtt": "2026-10-27-rtt",
    "orgs": "2026-10-27-orgs",
    "misc": "2026-10-27-misc",
    "outage": "2026-10-27-outage",
    "agents": "2026-10-27-agents",
    "move": "2026-10-27-logical8d-move",
}
PAGE_SCENARIOS = ["paths", "http", "overhead", "clients", "sessions", "rtt", "orgs", "misc", "outage", "agents", "volume", "move"]
ORGS = ["1", "2", "3", "4"]  # the organizations of the stand that ran the last pass (a fifth was out of reach)

_spec = importlib.util.spec_from_file_location("live_snapshot", ROOT / "bench" / "live-snapshot.py")
snap = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(snap)


def jsonl(path):
    out = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.startswith("{"):
            try:
                out.append(json.loads(line))
            except ValueError:
                pass
    return out


def runs_of(d):
    """the runs of a results directory in file order: consecutive lines with the same tags are one run"""
    runs, key = [], None
    files = sorted(f for f in (BENCH / d / "raw").glob("*.jsonl") if not f.name.startswith(("resources", "writer")))
    for f in files:
        for r in jsonl(f):
            t = r.get("tags", {})
            k = json.dumps(t, sort_keys=True)
            if k != key:
                runs.append({"scenario": t.get("scenario", d.split("-", 3)[-1]), "tags": t, "lines": []})
                key = k
            runs[-1]["lines"].append({x: v for x, v in r.items() if x != "tags"})
    # the repeats of a run collapse into the first one: the page shows a variant, not a repeat
    seen, out = set(), []
    for r in runs:
        t = {k: v for k, v in r["tags"].items() if k != "repeat"}
        k = json.dumps(t, sort_keys=True)
        if k not in seen:
            seen.add(k)
            out.append(r)
    return out


def resources_by_tags(d):
    f = BENCH / d / "raw" / "resources.jsonl"
    out = {}
    if f.exists():
        for rec in jsonl(f):
            t = {k: v for k, v in rec.get("tags", {}).items() if k != "repeat"}
            out.setdefault(json.dumps(t, sort_keys=True), []).append(rec)
    return out


PROG = re.compile(r"step=(copy|catch-up) .*?rows=(\d+) rows_total=(\d+) .*?elapsed=([0-9hms]+)")


def secs(s):
    m = re.fullmatch(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?", s)
    h, mi, se = (int(x or 0) for x in m.groups())
    return h * 3600 + mi * 60 + se


def move_runs(d):
    """a logical move under writes drawn from its log: the rows copied, then the changes caught up"""
    runs = []
    for f in sorted((BENCH / d / "raw").glob("move-*logical*.log")):
        if "rows3000000" not in f.name:
            continue
        for line in f.read_text(encoding="utf-8", errors="replace").splitlines():
            m = PROG.search(line)
            if not m:
                continue
            step, rows, total, el = m.group(1), int(m.group(2)), int(m.group(3)), secs(m.group(4))
            runs.append({"scenario": "move", "tags": {"scenario": "move", "config": "logical", "class": "moved", "clients": "8",
                                                      "op": f"move {step} {min(100, round(100 * rows / max(total, 1)))}% (rows/s)"},
                         "lines": [{"op": "move", "ops": rows, "qps": rows / max(el, 1), "p50_ms": 0, "p99_ms": 0, "errors": 0}]})
    return runs


def pick(runs, n):
    """n runs spread evenly over the list, the first and the last always in"""
    if len(runs) <= n:
        return runs
    return [runs[round(i * (len(runs) - 1) / (n - 1))] for i in range(n)] if n > 1 else runs[:1]


def headline(runs):
    ok = [r for r in runs if any(x.get("ops") for x in r["lines"])]
    if not ok:
        return ""
    def q(r):
        return sum(x.get("qps", 0) for x in r["lines"] if x.get("ops"))
    best = max(ok, key=q)
    errs = sum(x.get("errors", 0) for r in runs for x in r["lines"])
    spaced = lambda v: f"{v:,.0f}".replace(",", " ")
    return f"{len(runs)} variants measured\nfastest: {snap.variant(best['tags'])}, {spaced(q(best))} op/s" + (f"\n{spaced(errs)} failed operations in all" if errs else "\nno failed operations")


def plan(out, frames, scenarios):
    start = datetime.now(timezone.utc).replace(microsecond=0)
    ids = [s for s in PAGE_SCENARIOS if s in scenarios]
    for s in ids:
        if s not in SOURCES:
            sys.exit(f"no results for scenario {s}")
    per = max(2, (frames - 2) // len(ids) - 2)  # runs shown per scenario; with its start and its end that is the scenario's frames
    out.mkdir(parents=True, exist_ok=True)
    frames_out = []
    seq = [0]

    def when(n):
        return (start + timedelta(seconds=5 * n)).strftime("%Y-%m-%dT%H:%M:%SZ")

    def doc(name, data):
        seq[0] += 1
        f = f"{seq[0]:04d}-{name}.json"
        (out / f).write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
        return f

    def frame(writes):
        frames_out.append({"n": len(frames_out), "writes": writes})

    n0 = len(frames_out)
    frame([{"op": "update", "collection": "scenarios", "doc_id": s, "file": doc(f"reset-{s}", {"status": "pending", "started_at": {"__delete__": True}, "finished_at": {"__delete__": True}, "headline": {"__delete__": True}})} for s in PAGE_SCENARIOS]
          + [{"op": "update", "collection": "bench", "doc_id": "live", "file": doc("live-idle", {"state": "idle", "scenario": "", "variant": "replay of a measured pass", "links": {}, "active": [], "updated_at": when(0)})}])
    for s in ids:
        d = SOURCES[s]
        runs = move_runs(d) if s == "move" else runs_of(d)
        res = resources_by_tags(d)
        shown = pick(runs, per)
        t_idx = len(frames_out)
        frame([{"op": "update", "collection": "scenarios", "doc_id": s, "file": doc(f"start-{s}", {"status": "running", "started_at": when(t_idx)})},
               {"op": "set", "collection": "log", "doc_id": f"{when(t_idx).replace('-', '').replace(':', '').replace('T', '').replace('Z', '')}-{s}-start", "new": True,
                "file": doc(f"log-start-{s}", {"t": when(t_idx), "level": "info", "scenario": s, "text": f"{s}: {len(runs)} variants, shown {len(shown)}"})}])
        for r in shown:
            k = json.dumps({x: v for x, v in r["tags"].items() if x != "repeat"}, sort_keys=True)
            nodes = res.get(k, [])
            live = snap.build_doc(r, lambda scenario: ORGS, nodes, "running", when(len(frames_out)))
            live["scenario"] = s
            frame([{"op": "update", "collection": "bench", "doc_id": "live", "file": doc(f"live-{s}", live)}])
        t_idx = len(frames_out)
        frame([{"op": "update", "collection": "scenarios", "doc_id": s, "file": doc(f"done-{s}", {"status": "done", "finished_at": when(t_idx), "headline": headline(runs)})},
               {"op": "set", "collection": "log", "doc_id": f"{when(t_idx).replace('-', '').replace(':', '').replace('T', '').replace('Z', '')}-{s}-ok", "new": True,
                "file": doc(f"log-ok-{s}", {"t": when(t_idx), "level": "ok", "scenario": s, "text": f"{s}: done"})}])
    frame([{"op": "update", "collection": "bench", "doc_id": "live", "file": doc("live-end", {"state": "done", "updated_at": when(len(frames_out))})}])
    (out / "index.json").write_text(json.dumps({"frames": frames_out, "note": "apply frame by frame; pin updates with the version the page's document has"}, indent=1), encoding="utf-8")
    print(f"{len(frames_out)} frames, {seq[0]} documents in {out}; about {len(frames_out) * 5} s at 5 s a frame")


def show(d):
    idx = json.loads((Path(d) / "index.json").read_text(encoding="utf-8"))
    for fr in idx["frames"]:
        print(fr["n"], ", ".join(f"{w['op']} {w['collection']}/{w['doc_id']}" for w in fr["writes"]))


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--out", default=str(ROOT / "bench" / "replay-out"))
    p.add_argument("--frames", type=int, default=36)
    p.add_argument("--scenarios", default="paths,clients,rtt,move")
    s = sub.add_parser("show")
    s.add_argument("dir")
    a = ap.parse_args(argv)
    if a.cmd == "plan":
        plan(Path(a.out), a.frames, a.scenarios.split(","))
    else:
        show(a.dir)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
