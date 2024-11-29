import json
import sys
import duckdb


def ingest_data(file_path: str, db_path: str = "warehouse.db"):
    """
    Ingests vote data from a JSONL file into a DuckDB database.
    Deduplicates the records before insertion.

    :param file_path: Path to the input JSONL file.
    :param db_path: Path to the DuckDB database file.
    """
    try:
        # Connect to the DuckDB database
        conn = duckdb.connect(db_path)

        # Create schema and table if not exists
        conn.execute("CREATE SCHEMA IF NOT EXISTS blog_analysis;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS blog_analysis.votes (
                Id INTEGER PRIMARY KEY,
                PostId INTEGER,
                VoteTypeId INTEGER,
                CreationDate TIMESTAMP
            );
        """)

        # Load data from the JSONL file
        print(f"Reading data from {file_path}...")
        conn.execute(f"""
            INSERT INTO blog_analysis.votes
            SELECT DISTINCT 
                Id, 
                PostId, 
                VoteTypeId, 
                CAST(CreationDate AS TIMESTAMP)
            FROM read_json_auto('{file_path}');
        """)

        print("Data ingestion completed successfully.")
    except FileNotFoundError:
        print("Please download the dataset using 'poetry run exercise fetch-data'")
    except Exception as e:
        print(f"An error occurred during ingestion: {e}")
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    # Check for input file argument
    if len(sys.argv) < 2:
        print("Usage: python ingest.py <path_to_votes.jsonl>")
        sys.exit(1)

    # Run the ingestion process
    ingest_data(sys.argv[1])
