CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS pg16_docs (
    id              SERIAL PRIMARY KEY,
    chapter         TEXT,
    section         TEXT,
    subsection      TEXT,
    content         TEXT          NOT NULL,
    char_count      INTEGER,
    page_start      INTEGER,
    content_vector  vector(1536)  NOT NULL,
    content_tsv     TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', content)) STORED
);

CREATE INDEX IF NOT EXISTS idx_pg16_docs_hnsw
    ON pg16_docs USING hnsw (content_vector vector_cosine_ops);

CREATE INDEX IF NOT EXISTS idx_pg16_docs_gin
    ON pg16_docs USING gin (content_tsv);

CREATE INDEX IF NOT EXISTS idx_pg16_docs_chapter
    ON pg16_docs (chapter);

