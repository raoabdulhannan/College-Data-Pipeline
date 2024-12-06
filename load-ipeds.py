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


def load_ipeds(file_path):
    """
    Loads IPEDS data into the database.
    Processes and loads data into both Institutions and Crosswalks tables.

    The function performs the following steps:
    1. Extracts the year from the filename
    2. Reads and processes the CSV file
    3. Maps columns for both Institutions and Crosswalks tables
    4. Cleans existing data for the year
    5. Loads processed data into respective tables

    Args:
        file_path (str): Path to the IPEDS CSV file

    Raises:
        Exception: If any step in the data loading process fails
    """
    try:

        # Extract year from filename for data versioning
        year = extract_year_from_filename(file_path, 'ipeds')

        print("Reading CSV file...")
        ipeds_df = pd.read_csv(file_path, encoding="windows-1252")
        total_rows = len(ipeds_df)
        print(f"Total rows to process: {total_rows}")

        inst_cols = {
            "UNITID": "UNITID",
            "INSTNM": "INSTNM",
            "ADDR": "ADDR",
            "CITY": "CITY",
            "STABBR": "STABBR",
            "ZIP": "ZIP",
            "LATITUDE": "LATITUDE",
            "LONGITUD": "LONGITUDE",
            "CONTROL": "CONTROL",
            "OBEREG": "OBEREG",
            "CCBASIC": "CCBASIC",
            "CBSA": "CBSA",
            "CSA": "CSA",
            "COUNTYCD": "COUNTYCD"
        }

        cw_cols = {
            "UNITID": "UNITID",
            "OPEID": "OPEID"
        }

        # Process data for both tables with progress tracking
        print("Processing data for Institutions table...")
        with tqdm(total=total_rows,
                  desc="Processing Institutions data") as pbar:
            inst_data = process_chunk(ipeds_df, inst_cols)
            pbar.update(total_rows)

        print("Processing data for Crosswalks table...")
        with tqdm(total=total_rows, desc="Processing Crosswalks data") as pbar:
            crosswalk_data = process_chunk(ipeds_df, cw_cols)
            pbar.update(total_rows)

        # Establish database connection and begin loading process
        conn = connect_to_db()
        print("Connected to database successfully")

        # Remove existing data to prevent conflicts
        clear_existing_data(conn, year, 'ipeds')

        # Load processed data into respective tables
        print("Loading data into Institutions table...")
        with tqdm(total=total_rows, desc="Loading Institutions") as pbar:
            batch_insert(conn, "Institutions", inst_data, pbar=pbar)

        print("Loading data into Crosswalks table...")
        with tqdm(total=total_rows, desc="Loading Crosswalks") as pbar:
            batch_insert(conn, "Crosswalks", crosswalk_data, pbar=pbar)

        print("Data loading completed successfully")

    except Exception as e:
        print(f"Error loading IPEDS data: {str(e)}")
        raise

    finally:
        # Ensure database connection is closed even if an error occurs
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python load-ipeds.py <csv_file>")
        sys.exit(1)

    file_path = sys.argv[1]
    load_ipeds(file_path)
