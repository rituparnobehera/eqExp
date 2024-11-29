import duckdb


def test_duckdb_connection():
    """
    Test that DuckDB can establish a connection.
    """
    cursor = duckdb.connect("warehouse.db")
    assert list(cursor.execute("SELECT 1").fetchall()) == [(1,)], "DuckDB connection failed"


def test_votes_table_schema():
    """
    Test that the 'votes' table exists and has the correct schema.
    """
    conn = duckdb.connect("warehouse.db")
    try:
        result = conn.execute("""
            PRAGMA table_info('blog_analysis.votes');
        """).fetchall()
        expected_columns = [
            ("Id", "INTEGER", True),
            ("PostId", "INTEGER", False),
            ("VoteTypeId", "INTEGER", False),
            ("CreationDate", "TIMESTAMP", False)
        ]
        actual_columns = [(row[1], row[2], bool(row[5])) for row in result]
        assert actual_columns == expected_columns, f"Schema mismatch: {actual_columns} != {expected_columns}"
    finally:
        conn.close()


def test_sample_data_ingestion():
    """
    Test that the sample dataset is ingested correctly into the 'votes' table.
    """
    conn = duckdb.connect("warehouse.db")
    try:
        # Verify row count
        result = conn.execute("SELECT COUNT(*) FROM blog_analysis.votes;").fetchone()
        assert result[0] > 0, "No data found in the 'votes' table"

        # Verify some sample data
        result = conn.execute("SELECT * FROM blog_analysis.votes LIMIT 1;").fetchall()
        assert len(result) > 0, "Expected at least one row in the 'votes' table"
    finally:
        conn.close()
