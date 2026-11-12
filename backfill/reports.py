"""Run the demonstration queries in sql/sample_queries.sql and render them as markdown."""
from __future__ import annotations

from pathlib import Path


def run_sample_queries(conn, sql_file: Path) -> str:
    text = Path(sql_file).read_text()
    blocks, current, title = [], [], None
    for line in text.splitlines():
        if line.startswith("-- name:"):
            if title and current:
                blocks.append((title, "\n".join(current)))
            title, current = line.split(":", 1)[1].strip(), []
        elif title is not None:
            current.append(line)
    if title and current:
        blocks.append((title, "\n".join(current)))
    out = ["# Sample query results", ""]
    for title, sql in blocks:
        sql = sql.strip().rstrip(";")
        cur = conn.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        out += [f"## {title}", "", "```sql", sql, "```", "",
                "| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
        out += ["| " + " | ".join("" if v is None else str(v) for v in r) + " |" for r in rows]
        out.append("")
    return "\n".join(out)
