"""Stable-identity helpers ported verbatim from the pipeline (scraper.py /
rubric.py). The Apply URL is NOT a stable identity — rubric.cmd_score rewrites a
job's url to its canonical ATS url on resolution, and `reposted` fires when a job
resurfaces under a new url. The stable identity is the seen-key:
    canon_company(company) | norm(title) | norm(city)

This copy only needs INTERNAL consistency (for state reconciliation on ingest);
it does not need to match the pipeline's seen.jsonl ledger.
"""
import re

COMPANY_CANON = {"amazoncom": "amazon", "amazon web services": "amazon", "aws": "amazon",
 "the boeing company": "boeing", "microsoft corporation": "microsoft", "t mobile": "tmobile",
 "tmobile usa": "tmobile", "meta platforms": "meta", "crusoe energy": "crusoe",
 "salesforce inc": "salesforce", "ntt data inc": "ntt data"}


def norm(s):
    return re.sub(r"[^a-z0-9 ]", "", (s or "").lower()).strip()


def canon_company(c):
    x = norm((c or "").replace(".com", " ").replace(",", " "))
    x = re.sub(r"\b(inc|llc|corp|corporation|co|ltd|group|holdings|company)\b", " ", x)
    x = re.sub(r"^the\s+", "", re.sub(r"\s+", " ", x).strip())
    return COMPANY_CANON.get(x, x)


def seen_key(company, title, location):
    city = (re.split(r"[,(;]", location or "") or [""])[0]
    return f"{canon_company(company)}|{norm(title)}|{norm(city)}"
