import type { ReactNode } from "react";

// Common HTML entities decoded to literal text BEFORE display. Scraped JD text
// is rendered as plain text (never dangerouslySetInnerHTML), so entities that
// slipped through the pipeline must be un-escaped here for the reader.
const ENTITIES: Array<[string, string]> = [
  ["&nbsp;", " "],
  ["&amp;", "&"],
  ["&lt;", "<"],
  ["&gt;", ">"],
  ["&#39;", "'"],
  ["&quot;", '"'],
];

export function decodeEntities(text: string): string {
  if (!text) return "";
  let out = text;
  for (const [from, to] of ENTITIES) {
    out = out.split(from).join(to);
  }
  return out;
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Split `text` on case-insensitive matches of any of `terms` and wrap each match
 * in <mark>. Terms are escaped before building the RegExp, so user/config skill
 * strings can never inject regex. Returns a ReactNode (array of strings + marks).
 */
export function highlightText(text: string, terms: string[]): ReactNode {
  const decoded = decodeEntities(text ?? "");
  const clean = Array.from(
    new Set((terms ?? []).map((t) => (t ?? "").trim()).filter((t) => t.length > 0)),
  );
  if (clean.length === 0) return decoded;

  // Longest-first so overlapping terms prefer the longer match.
  clean.sort((a, b) => b.length - a.length);
  const pattern = clean.map(escapeRegExp).join("|");

  let re: RegExp;
  try {
    re = new RegExp(`(${pattern})`, "gi");
  } catch {
    return decoded;
  }

  const parts = decoded.split(re);
  return parts.map((part, i) =>
    i % 2 === 1 ? (
      <mark key={i} className="hl">
        {part}
      </mark>
    ) : (
      part
    ),
  );
}
