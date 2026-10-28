#!/usr/bin/env python3
"""Fails if anything in the results or the measurement documents says where a server stood or whose it was.

    python3 bench/check-clean.py                 research/bench, bench/, the measurement plans, README.md, research/ANALYSIS.md
    python3 bench/check-clean.py DIR_OR_FILE...  only these

What the measurements may name is a server's role (cloud, proxy, load, org1 ...), its cores, memory and disk, and for an
organization's database the RTT from the cloud ("the organization at about 10 ms"). Not allowed: an address (the loopback
and 0.0.0.0 aside), user@host, a hosting provider or its tariff, a region code, a city or a country, a host name. Run it
before committing anything under research/bench (bench/run-all.sh and every scenario's end run it too).
Exit code 0: clean; 1: findings (printed as file:line: what was matched).
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

DEFAULT = ["research/bench", "bench", "tasks/load-plan.md", "tasks/load-plan-servers.md", "README.md", "research/ANALYSIS.md"]
SKIP_DIRS = {"state", "state-archive", "dist", ".git", "node_modules", "__pycache__"}
SKIP_FILES = {"check-clean.py"}  # it lists what it looks for
TEXT = {".md", ".txt", ".json", ".jsonl", ".csv", ".log", ".sh", ".py", ".go", ".example", ".yml", ".yaml", ".svg", ".php", ".inv", ""}

PROVIDERS = r"timeweb|twc|hetzner|ovh|digitalocean|vultr|linode|selectel|yandex\s*cloud|vk\s*cloud|cloud\.ru|reg\.ru|beget|aeza"  # hosting providers; the clouds named as products (RDS, Azure) are not places
TARIFFS = r"cloud\s+(dedic|msk|ekb|nsk|spb|de|nl|pl|kz)\b|cloud-\d{2,3}\b|high-\d{2,3}\b"
REGIONS = r"\b(ru|kz|de|nl|pl|us|eu|ua|by|fi|fr|gb|uk|se|tr|ge|am|uz)-\d\b"
# The Cyrillic names of places below are what the check searches for (the same places, in Russian): they stay in Cyrillic on purpose.
PLACES = (r"mosc?ow|(?<![а-яё])(москв|московск|санкт|петербург|новосибир|екатеринбург|казан|франкфурт|амстердам|нидерланд|казахстан|варшав|польш)|"
          r"saint[- ]petersburg|novosibirsk|yekaterinburg|kazan|frankfurt|amsterdam|netherlands|kazakhstan|warsaw|poland|"
          r"(?<![а-яё])(спб|мск|екб)(?![а-яё])")
HOSTS = r"\bmsk-\d|\bspb-\d|\bvm-[0-9a-z]{4,}\b"

IPV4 = re.compile(r"(?<![\w.])(\d{1,3}\.){3}\d{1,3}(?![\w.]*\d)")
USERHOST = re.compile(r"\b[a-z_][a-z0-9_-]*@(?!CLOUD_|PROXY_|LOAD_|ORG\d_|HOST|<host>|example\.)[a-z0-9][a-z0-9.-]*\b", re.I)
WORDS = re.compile("|".join([PROVIDERS, TARIFFS, REGIONS, PLACES, HOSTS]), re.I)
LOOPBACK = {"127.0.0.1", "0.0.0.0"}


def files(paths):
    for p in paths:
        p = (ROOT / p) if not Path(p).is_absolute() else Path(p)
        if p.is_file():
            yield p
        elif p.is_dir():
            for f in sorted(p.rglob("*")):
                if f.is_file() and not (set(f.relative_to(p).parts[:-1]) & SKIP_DIRS) and f.suffix in TEXT and f.name not in SKIP_FILES:
                    yield f


def main(argv):
    findings = []
    for f in files(argv or DEFAULT):
        try:
            lines = f.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for n, line in enumerate(lines, 1):
            hits = [m.group(0) for m in IPV4.finditer(line) if m.group(0) not in LOOPBACK and all(int(x) <= 255 for x in m.group(0).split("."))]
            hits += [m.group(0) for m in USERHOST.finditer(line) if not m.group(0).startswith(("postgres@", "git@", "docker@"))]
            hits += [m.group(0) for m in WORDS.finditer(line)]
            if hits:
                findings.append((f, n, hits[:3]))
    if findings:
        shown = {}
        for f, n, hits in findings:
            shown[f] = shown.get(f, 0) + 1
            if shown[f] <= 3:
                print(f"{f.relative_to(ROOT)}:{n}: {', '.join(hits)}")
        print(f"\ncheck-clean: {len(findings)} line(s) in {len(shown)} file(s) name an address, a provider, a region or a place", file=sys.stderr)
        return 1
    print("check-clean: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
