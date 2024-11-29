import duckdb

def verify_data(db_path="warehouse.db"):
    try:
        # Connect to the DuckDB database
        conn = duckdb.connect(db_path)

        # Query to fetch sample data
        result = conn.execute("SELECT * FROM blog_analysis.outlier_weeks;").fetchall()

        # Print the results
        print("Sample data from blog_analysis.votes:")
        for row in result:
            print(row)
    except Exception as e:
        print(f"An error occurred while verifying data: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    verify_data()
