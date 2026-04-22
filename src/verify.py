import os
import psycopg
from dotenv import load_dotenv

load_dotenv()
DB_NAME = os.environ.get("DB_NAME", "pg16_rag")

TEST_QUERIES = [
    "MVCC concurrency control",
    "VACUUM autovacuum bloat",
    "WAL write ahead log replication",
    "index types btree hash",
]

def run_checks():
    failures = []

    with psycopg.connect(f"dbname={DB_NAME}") as conn:
        with conn.cursor() as cur:

            # 1. Row count
            cur.execute("SELECT COUNT(*) FROM pg16_docs")
            count = cur.fetchone()[0]
            print(f"Row count: {count:,}")
            if not (4500 <= count <= 20000):
                failures.append(f"Row count {count} out of expected range")
            else:
                print("  PASS")

            # 2. NULL vectors
            cur.execute("SELECT COUNT(*) FROM pg16_docs WHERE content_vector IS NULL")
            nulls = cur.fetchone()[0]
            print(f"NULL vectors: {nulls}")
            if nulls > 0:
                failures.append(f"{nulls} rows have NULL content_vector")
            else:
                print("  PASS")

            # 3. Distinct chapters
            cur.execute("SELECT COUNT(DISTINCT chapter) FROM pg16_docs")
            chapters = cur.fetchone()[0]
            print(f"Distinct chapters: {chapters}")
            if chapters < 20:
                failures.append(f"Only {chapters} distinct chapters (need ≥20)")
            else:
                print("  PASS")

            # 4. Test queries
            for q in TEST_QUERIES:
                cur.execute("""
                    SELECT COUNT(*) FROM pg16_docs
                    WHERE content_tsv @@ plainto_tsquery('english', %s)
                """, (q,))
                hits = cur.fetchone()[0]
                print(f"Query '{q}': {hits} results")
                if hits < 3:
                    failures.append(f"Query '{q}' returned only {hits} results (need ≥3)")
                else:
                    print("  PASS")

    print()
    if failures:
        print("FAILURES:")
        for f in failures:
            print(f"  - {f}")
        exit(1)
    else:
        print("All checks passed.")
        exit(0)

if __name__ == "__main__":
    run_checks()

