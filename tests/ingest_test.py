import duckdb
from equalexperts_dataeng_exercise.ingest import ingest_data
from pathlib import Path

def test_ingest_data_with_sample(setup_empty_db):
    """
    Test that ingest_data correctly loads data into the DuckDB database.
    """
    db_path = setup_empty_db
    sample_data_path = Path("tests/test-resources") / "samples-votes.jsonl"

    # Run the ingestion process
    ingest_data(sample_data_path, db_path)

    # Verify data was ingested
    conn = duckdb.connect(db_path)
    try:
        # Check row count
        result = conn.execute("SELECT COUNT(*) FROM blog_analysis.votes;").fetchone()
        assert result[0] > 0, "Expected data to be ingested into the votes table"

        # Verify schema and sample data
        schema_result = conn.execute("PRAGMA table_info('blog_analysis.votes');").fetchall()
        expected_columns = [
            ("Id", "INTEGER", True),
            ("PostId", "INTEGER", False),
            ("VoteTypeId", "INTEGER", False),
            ("CreationDate", "TIMESTAMP", False)
        ]
        actual_columns = [(row[1], row[2], bool(row[5])) for row in schema_result]
        assert actual_columns == expected_columns, f"Schema mismatch: {actual_columns} != {expected_columns}"

        # Verify sample data exists
        data_result = conn.execute("SELECT * FROM blog_analysis.votes LIMIT 1;").fetchall()
        for row in data_result:
            print(row)
        assert len(data_result) > 0, "Expected at least one row in the votes table"
    finally:
        conn.close()
