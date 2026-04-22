import os
import fitz
from openai import OpenAI
from dotenv import load_dotenv
import psycopg

load_dotenv()
client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

DB_NAME = os.environ.get("DB_NAME", "pg16_rag")
PDF_PATH = "postgresql-16-A4.pdf"

CONTENT_START_PAGE = 31
CONTENT_END_PAGE   = 2995

CHAPTER_SIZE    = 24.0
SECTION_SIZE    = 20.0
SUBSECTION_SIZE = 17.0
SKIP_SIZE       = 8.5

MIN_CHUNK_CHARS = 150
MAX_CHUNK_CHARS = 1800
OVERLAP_SPANS   = 5
EMBED_BATCH     = 100


def is_bold(flags: int) -> bool:
    return bool(flags & 2**4)


def extract_chunks(pdf_path: str) -> list[dict]:
    doc = fitz.open(pdf_path)
    chunks = []

    current_chapter    = ""
    current_section    = ""
    current_subsection = ""
    span_buffer        = []

    def flush_with_splits(chap, sec, subsec, spans, page):
        if not spans:
            return
        while len(spans) > 0:
            batch = spans[:MAX_CHUNK_CHARS]
            text = " ".join(batch).strip()
            if len(text) >= MIN_CHUNK_CHARS:
                chunks.append({
                    "chapter":    chap,
                    "section":    sec,
                    "subsection": subsec,
                    "content":    text,
                    "char_count": len(text),
                    "page_start": page,
                })
            spans = spans[max(1, len(batch) - OVERLAP_SPANS):]
            if len(" ".join(spans).strip()) < MIN_CHUNK_CHARS:
                break

    print(f"Extracting chunks from {pdf_path}...")

    for page_num in range(CONTENT_START_PAGE, CONTENT_END_PAGE + 1):
        page = doc[page_num]
        blocks = page.get_text("dict")["blocks"]

        for block in blocks:
            if block["type"] != 0:
                continue
            for line in block["lines"]:
                for span in line["spans"]:
                    size  = span["size"]
                    bold  = is_bold(span["flags"])
                    text  = span["text"].strip()

                    if not text:
                        continue
                    if size <= SKIP_SIZE:
                        continue

                    if size >= CHAPTER_SIZE and bold:
                        flush_with_splits(current_chapter, current_section,
                                          current_subsection, span_buffer, page_num + 1)
                        span_buffer        = []
                        current_chapter    = text
                        current_section    = ""
                        current_subsection = ""

                    elif size >= SECTION_SIZE and bold:
                        flush_with_splits(current_chapter, current_section,
                                          current_subsection, span_buffer, page_num + 1)
                        span_buffer        = []
                        current_section    = text
                        current_subsection = ""

                    elif size >= SUBSECTION_SIZE and bold:
                        flush_with_splits(current_chapter, current_section,
                                          current_subsection, span_buffer, page_num + 1)
                        span_buffer        = []
                        current_subsection = text

                    else:
                        span_buffer.append(text)

                        joined = " ".join(span_buffer)
                        if len(joined) >= MAX_CHUNK_CHARS:
                            flush_with_splits(current_chapter, current_section,
                                              current_subsection, span_buffer, page_num + 1)
                            span_buffer = span_buffer[-OVERLAP_SPANS:]

    flush_with_splits(current_chapter, current_section,
                      current_subsection, span_buffer, CONTENT_END_PAGE + 1)

    doc.close()
    return chunks


def embed_batch(texts: list[str]) -> list[list[float]]:
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=texts,
    )
    return [r.embedding for r in response.data]


def ingest():
    chunks = extract_chunks(PDF_PATH)
    total  = len(chunks)
    print(f"Extracted {total:,} chunks")

    cost_estimate = (sum(c["char_count"] for c in chunks) / 4) / 1_000_000 * 0.02
    print(f"Estimated embedding cost: ${cost_estimate:.4f}")

    with psycopg.connect(f"dbname={DB_NAME}") as conn:
        with conn.cursor() as cur:
            inserted = 0
            for i in range(0, total, EMBED_BATCH):
                batch   = chunks[i : i + EMBED_BATCH]
                texts   = [c["content"] for c in batch]
                vectors = embed_batch(texts)

                for chunk, vec in zip(batch, vectors):
                    cur.execute("""
                        INSERT INTO pg16_docs
                            (chapter, section, subsection, content, char_count, page_start, content_vector)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        chunk["chapter"],
                        chunk["section"],
                        chunk["subsection"],
                        chunk["content"],
                        chunk["char_count"],
                        chunk["page_start"],
                        vec,
                    ))

                conn.commit()
                inserted += len(batch)
                pct = inserted / total * 100
                print(f"  {inserted}/{total} chunks ({pct:.0f}%)")

    print(f"Done. {inserted:,} chunks stored in pg16_docs.")


if __name__ == "__main__":
    ingest()

