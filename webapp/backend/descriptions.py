"""Streaming description join for the 144MB results/descriptions.jsonl.

NEVER load the file whole. Stream line-by-line, json.loads per line, skip
malformed lines, early-break once every wanted url is satisfied, cap desc length
at 100_000 chars. Peak memory is ~one line plus the wanted set.
"""
import json

from . import config

MAX_DESC_CHARS = 100_000


def stream_descriptions(wanted, results_dir=None):
    """Given a set/iterable of wanted urls, return {url: desc} for those found in
    descriptions.jsonl. Only the wanted urls are ever held; the file is read once.
    """
    remaining = set(wanted)
    out = {}
    if not remaining:
        return out
    path = (results_dir or config.RESULTS) / "descriptions.jsonl"
    if not path.exists():
        return out
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue  # skip malformed lines
            url = obj.get("url")
            if url in remaining:
                desc = obj.get("desc") or ""
                if not isinstance(desc, str):
                    desc = str(desc)
                out[url] = desc[:MAX_DESC_CHARS]
                remaining.discard(url)
                if not remaining:
                    break  # early-break: every wanted url satisfied
    return out
