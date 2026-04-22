import os
import psycopg
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()
client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
DB_NAME = os.environ.get("DB_NAME", "pg16_rag")
TOP_K   = 15

SYSTEM_PROMPT = """You are a PostgreSQL DBA assistant. Answer questions using ONLY the provided
PostgreSQL 16 documentation excerpts. Always cite the chapter, section, and
PDF page number your answer comes from. If the documentation excerpt is
insufficient to fully answer the question, say so explicitly rather than
guessing. Do not use knowledge outside the provided excerpts."""

_RRF_CTES = """
WITH vector_results AS (
    SELECT id, content, chapter, section, subsection, page_start,
           ROW_NUMBER() OVER (
               ORDER BY content_vector <=> %s::vector
           ) AS rank
    FROM pg16_docs
    ORDER BY content_vector <=> %s::vector
    LIMIT 20
),
fts_results AS (
    SELECT id, content, chapter, section, subsection, page_start,
           ROW_NUMBER() OVER (
               ORDER BY ts_rank_cd(content_tsv,
                        plainto_tsquery('english', %s)) DESC
           ) AS rank
    FROM pg16_docs
    WHERE content_tsv @@ plainto_tsquery('english', %s)
    LIMIT 20
),
rrf AS (
    SELECT
        COALESCE(v.id, f.id)                 AS id,
        COALESCE(v.content, f.content)       AS content,
        COALESCE(v.chapter, f.chapter)       AS chapter,
        COALESCE(v.section, f.section)       AS section,
        COALESCE(v.subsection, f.subsection) AS subsection,
        COALESCE(v.page_start, f.page_start) AS page_start,
        (%s::float * COALESCE(1.0/(60.0 + v.rank), 0)) +
        (%s::float * COALESCE(1.0/(60.0 + f.rank), 0)) AS rrf_score
    FROM vector_results v
    FULL OUTER JOIN fts_results f ON v.id = f.id
)
"""

HYBRID_SQL = _RRF_CTES + """
SELECT id, content, chapter, section, subsection, page_start, rrf_score
FROM rrf
ORDER BY rrf_score DESC
LIMIT %s
"""

HYBRID_SQL_CHAPTER = _RRF_CTES + """
SELECT id, content, chapter, section, subsection, page_start, rrf_score
FROM rrf
WHERE chapter ILIKE %s
ORDER BY rrf_score DESC
LIMIT %s
"""


def classify_query(question: str) -> tuple[float, float]:
    q = question.lower()

    if any(s in q for s in ["vs ", "versus", "difference between",
                              "compare", "better", "when to use",
                              "pros and cons", "trade-off", "which one"]):
        return 0.7, 0.3

    if any(s in q for s in ["syntax", "how to write", "example",
                              "create ", "alter ", "drop ", "select ",
                              "parameter", "option", "flag",
                              "what is the default", "syntax of"]):
        return 0.2, 0.8

    if any(s in q for s in ["why", "not running", "not working", "slow",
                              "bloat", "stuck", "failed", "error", "issue",
                              "problem", "debug", "lock", "wait", "blocking",
                              "keeps", "high cpu", "high memory"]):
        return 0.6, 0.4

    return 0.7, 0.3


def embed(text: str) -> list[float]:
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=[text],
    )
    return response.data[0].embedding


def search(conn, question: str, vec_w: float, fts_w: float,
           chapter_filter: str = None) -> list[dict]:
    q_vec = embed(question)

    if chapter_filter:
        sql    = HYBRID_SQL_CHAPTER
        params = (q_vec, q_vec, question, question, vec_w, fts_w, chapter_filter, TOP_K)
    else:
        sql    = HYBRID_SQL
        params = (q_vec, q_vec, question, question, vec_w, fts_w, TOP_K)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    return [
        {
            "content":    r[1],
            "chapter":    r[2],
            "section":    r[3],
            "subsection": r[4],
            "page_start": r[5],
        }
        for r in rows
    ]


def format_context(chunks: list[dict]) -> str:
    parts = []
    for c in chunks:
        header = f"[{c['chapter']} › {c['section']} › {c['subsection']} | PDF page {c['page_start']}]"
        parts.append(f"{header}\n{c['content']}")
    return "\n\n---\n\n".join(parts)


def answer(question: str, chunks: list[dict]) -> str:
    context = format_context(chunks)
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": f"Context:\n{context}\n\nQuestion: {question}"},
        ],
    )
    return response.choices[0].message.content


def main():
    conn = psycopg.connect(f"dbname={DB_NAME}")

    print("PostgreSQL 16 Documentation RAG")
    print("Commands: type question | 'chapter:<name> <question>' to filter | 'quit'")
    print("-" * 60)

    try:
        while True:
            try:
                print()
                question = input("Question: ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nBye.")
                break

            if not question:
                continue
            if question.lower() in ("quit", "exit", "q"):
                print("Bye.")
                break

            chapter_filter = None
            if question.startswith("chapter:"):
                parts = question[8:].split(" ", 1)
                if len(parts) == 2:
                    chapter_filter = f"%{parts[0]}%"
                    question       = parts[1]

            vec_w, fts_w = classify_query(question)
            chunks = search(conn, question, vec_w, fts_w, chapter_filter)

            if not chunks:
                print("No results found.")
                continue

            print("Answer:", answer(question, chunks))
    finally:
        conn.close()


if __name__ == "__main__":
    main()

