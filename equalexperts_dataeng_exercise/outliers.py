import duckdb


def calculate_outliers(db_path: str = "warehouse.db"):
    """
    Calculates outlier weeks based on the votes data and creates a view in the DuckDB database.

    :param db_path: Path to the DuckDB database file.
    """
    conn = duckdb.connect(db_path)
    try:
        # SQL query to create the outlier_weeks view
        conn.execute("""
            CREATE OR REPLACE VIEW blog_analysis.outlier_weeks AS
WITH weekly_votes AS (
    SELECT
        EXTRACT(YEAR FROM CreationDate) AS Year,
        CASE 
            WHEN EXTRACT(WEEK FROM CreationDate) = 52 AND EXTRACT(MONTH FROM CreationDate) = 1 THEN 0
            ELSE EXTRACT(WEEK FROM CreationDate)
        END AS WeekNumber,
        COUNT(*) AS VoteCount
    FROM blog_analysis.votes
    GROUP BY Year, WeekNumber
),
mean_votes AS (
    SELECT AVG(VoteCount) AS AvgVoteCount FROM weekly_votes
)
SELECT
    Year,
    WeekNumber,
    VoteCount
FROM weekly_votes, mean_votes
WHERE ABS(1 - (VoteCount * 1.0 / AvgVoteCount)) > 0.2
ORDER BY Year, WeekNumber;

        """)
        print("Outlier weeks view created successfully.")
    except Exception as e:
        print(f"An error occurred while calculating outliers: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    calculate_outliers()
