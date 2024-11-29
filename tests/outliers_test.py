import duckdb
from equalexperts_dataeng_exercise.outliers import calculate_outliers


def test_outlier_weeks_view_exists(setup_sample_data):
    """
    Test that the 'outlier_weeks' view is created successfully.
    """
    db_path = setup_sample_data

    # Run the outlier calculation process
    calculate_outliers(db_path)

    # Verify the view exists
    conn = duckdb.connect(db_path)
    try:
        result = conn.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_type='VIEW' AND table_name='outlier_weeks' AND table_schema='blog_analysis';
        """).fetchone()
        assert result is not None, "Expected view 'outlier_weeks' to exist"
    finally:
        conn.close()


def test_outlier_weeks_has_data(setup_sample_data):
    """
    Test that the 'outlier_weeks' view contains data.
    """
    db_path = setup_sample_data

    # Run the outlier calculation process
    calculate_outliers(db_path)

    # Verify the view has data
    conn = duckdb.connect(db_path)
    try:
        result = conn.execute("SELECT COUNT(*) FROM blog_analysis.outlier_weeks;").fetchone()
        assert result[0] > 0, "Expected 'outlier_weeks' view to have data"
    finally:
        conn.close()


def test_outlier_weeks_correctness(setup_sample_data):
    """
    Test that the 'outlier_weeks' view contains correct outlier data based on the sample dataset.
    """
    db_path = setup_sample_data

    # Run the outlier calculation process
    calculate_outliers(db_path)

    # Expected outliers based on the given results
    expected_outliers = [
        (2022, 0, 1),  # Year 2022, Week 0, with 1 vote
        (2022, 1, 3),  # Year 2022, Week 1, with 3 votes
        (2022, 2, 3),  # Year 2022, Week 2, with 3 votes
        (2022, 5, 1),  # Year 2022, Week 5, with 1 vote
        (2022, 6, 1),  # Year 2022, Week 6, with 1 vote
        (2022, 8, 1)   # Year 2022, Week 8, with 1 vote
    ]

    # Fetch actual outliers from the view
    conn = duckdb.connect(db_path)
    try:
        actual_outliers = conn.execute("""
            SELECT Year, WeekNumber, VoteCount
            FROM blog_analysis.outlier_weeks
            ORDER BY Year, WeekNumber;
        """).fetchall()

        # Compare actual results with expected results
        assert actual_outliers == expected_outliers, f"Outliers mismatch: Actual: {actual_outliers}, Expected: {expected_outliers}"
    finally:
        conn.close()
