import pandas as pd
import sys
from tqdm import tqdm
from load import (
    connect_to_db,
    batch_insert,
    process_chunk,
    extract_year_from_filename,
    clear_existing_data
)


def load_scorecard(file_path):
    """
    Loads College Scorecard data into the database.
    Processes and loads data into both College_Scorecard_Annual
    and Financial_Data tables.

    The College Scorecard dataset contains comprehensive information about US
    colleges, including admissions rates, graduation rates, faculty data, and
    various financial metrics like tuition and student debt.

    Processing Steps:
    1. Extracts academic year from filename
    2. Establishes database connection
    3. Clears existing year's data
    4. Processes institutional metrics
    5. Processes financial data
    6. Loads both datasets into their respective tables

    Args:
        file_path (str): Path to the College Scorecard CSV file

    Raises:
        Exception: If any step in the data loading process fails
    """
    try:

        # Extract academic year from filename (e.g., 2018_19 becomes 2019)
        year = extract_year_from_filename(file_path, 'scorecard')

        # Establish database connection and prepare for new data
        conn = connect_to_db()
        print("Connected to database successfully")
        clear_existing_data(conn, year, 'scorecard')

        print("Reading CSV file...")
        df = pd.read_csv(file_path, encoding="windows-1252", low_memory=False)
        total_rows = len(df)
        print(f"Total rows to process: {total_rows}")

        scorecard_cols = {
            "UNITID": "UNITID",
            "ACCREDAGENCY": "ACCREDAGENCY",
            "PREDDEG": "PREDDEG",
            "HIGHDEG": "HIGHDEG",
            "ADM_RATE": "ADM_RATE",
            "C150_4": "C150_4",
            "C200_4": "C200_4",
            "AVGFACSAL": "AVGFACSAL"
        }

        financial_cols = {
            "OPEID": "OPEID",
            "TUITIONFEE_IN": "TUITIONFEE_IN",
            "TUITIONFEE_OUT": "TUITIONFEE_OUT",
            "TUITIONFEE_PROG": "TUITIONFEE_PROG",
            "PCTPELL": "PCTPELL",
            "DEBT_MDN": "DEBT_MDN",
            "RPY_3YR_RT": "RPY_3YR_RT",
            "CDR2": "CDR2",
            "CDR3": "CDR3",
            "MD_EARN_WNE_P8": "MD_EARN_WNE_P8"
        }

        print("Processing College Scorecard data...")
        with tqdm(total=total_rows, desc="Processing Scorecard data") as pbar:

            # Extract and deduplicate institutional metrics
            scorecard_df = df[list(scorecard_cols.keys())].copy()

            # Keep first occurrence for each institution
            scorecard_data = scorecard_df.drop_duplicates(
                subset=['UNITID'], keep='first')
            scorecard_data = process_chunk(scorecard_data, scorecard_cols)

            # Add academic year reference
            scorecard_data['YEAR'] = year
            pbar.update(total_rows)

        print("Processing Financial data...")
        with tqdm(total=total_rows, desc="Processing Financial data") as pbar:

            # Extract and deduplicate financial metrics
            financial_df = df[list(financial_cols.keys())].copy()

            # Keep first occurrence for each OPEID
            financial_data = financial_df.drop_duplicates(
                subset=['OPEID'], keep='first')
            financial_data = process_chunk(financial_data, financial_cols)

            # Add academic year reference
            financial_data['YEAR'] = year
            pbar.update(total_rows)

        # Load processed data into respective tables
        print("Loading data into College_Scorecard_Annual table...")
        with tqdm(total=len(scorecard_data),
                  desc="Loading Scorecard data") as pbar:

            # Dropping rows with NA OPEID since it can't be NULL
            financial_data = financial_data.dropna(subset=['OPEID'])
            batch_insert(conn, "College_Scorecard_Annual", scorecard_data,
                         pbar=pbar)

        print("Loading data into Financial_Data table...")
        with tqdm(total=len(financial_data),
                  desc="Loading Financial data") as pbar:
            batch_insert(conn, "Financial_Data", financial_data, pbar=pbar)

        print("Data loading completed successfully")

    except Exception as e:
        print(f"Error loading College Scorecard data: {str(e)}")
        raise

    finally:

        # Ensure database connection is closed even if an error occurs
        if 'conn' in locals():
            conn.close()
            print("Database connection closed")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python load-scorecard.py <csv_file>")
        sys.exit(1)

    file_path = sys.argv[1]
    load_scorecard(file_path)
