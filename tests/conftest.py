import os
import duckdb
import pytest
from pathlib import Path

@pytest.fixture
def setup_empty_db():
    """
    Fixture to set up an empty DuckDB database for testing ingestion.
    """
    db_path = "test_warehouse.db"
    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS blog_analysis;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS blog_analysis.votes (
            Id INTEGER PRIMARY KEY,
            PostId INTEGER,
            VoteTypeId INTEGER,
            CreationDate TIMESTAMP
        );
    """)
    conn.close()
    yield db_path
    os.remove(db_path)


@pytest.fixture
def setup_sample_data():
    """
    Fixture to set up a DuckDB database with sample data for testing.
    Loads data from the test-resources folder.
    """
    db_path = "test_warehouse.db"
    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS blog_analysis;")
    conn.execute("""
        CREATE TABLE blog_analysis.votes (
            Id INTEGER PRIMARY KEY,
            PostId INTEGER,
            VoteTypeId INTEGER,
            CreationDate TIMESTAMP
        );
    """)
    sample_data_path = Path("tests/test-resources") / "samples-votes.jsonl"
    conn.execute(f"""
        INSERT INTO blog_analysis.votes
        SELECT DISTINCT Id, PostId, VoteTypeId, CAST(CreationDate AS TIMESTAMP)
        FROM read_json_auto('{sample_data_path}');
    """)
    conn.close()
    yield db_path
    os.remove(db_path)
