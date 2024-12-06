import pandas as pd
import psycopg
import re
from credentials import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST


def extract_year_from_filename(filename, data_source):
    """
    Extract the year from filename based on the data source format.
    IPEDS format: hd2019.csv -> 2019
    Scorecard format: MERGED2018_19_PP.csv -> 2019

    Args:
        filename (str): Name of the input file
        data_source (str): Type of data source ('ipeds' or 'scorecard')

    Returns:
        int: Extracted year from the filename

    Raises:
        ValueError: If filename format doesn't match expected pattern
    """
    if data_source.lower() == 'ipeds':
        match = re.search(r'hd(\d{4})\.csv', filename)
        if not match:
            raise ValueError(f"Invalid IPEDS filename format: {filename}")
        return int(match.group(1))

    elif data_source.lower() == 'scorecard':
        match = re.search(r'MERGED(\d{4})_(\d{2})_PP\.csv', filename)
        if not match:
            raise ValueError(f"Invalid College Scorecard filename format:"
                             f" {filename}")

        # For scorecard, we add 1 since data spans academic year
        return int(match.group(1)) + 1

    raise ValueError(f"Invalid data source: {data_source}")


def connect_to_db():
    """
    Establishes and returns a database connection using given credentials.

    Returns:
        psycopg.Connection: Database connection object

    Raises:
        Exception: If connection cannot be established
    """
    try:
        return psycopg.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST
        )
    except Exception as e:
        print(f"Database connection error: {str(e)}")
        raise


def clear_existing_data(conn, year, data_source):
    """
    Removes existing data for the specified year based on the data source.
    This ensures clean data loading and prevents primary key violations.

    For IPEDS: Removes all institution and crosswalk data
    For Scorecard: Removes specific year's data from relevant tables

    Args:
        conn (psycopg.Connection): Database connection
        year (int): Year of data to clear
        data_source (str): Type of data source ('ipeds' or 'scorecard')

    Raises:
        Exception: If data deletion fails
    """
    try:
        with conn.cursor() as cur:
            print(f"Removing existing {data_source} data for year {year}...")

            if data_source.lower() == 'ipeds':

                # Delete crosswalks first to maintain referential integrity
                cur.execute("DELETE FROM Crosswalks WHERE UNITID IN"
                            "(SELECT UNITID FROM Institutions)")
                cur.execute("DELETE FROM Institutions")

            elif data_source.lower() == 'scorecard':
                cur.execute(
                    "DELETE FROM College_Scorecard_Annual WHERE year = %s",
                    (year,))
                cur.execute(
                    "DELETE FROM Financial_Data WHERE year = %s",
                    (year,))

            conn.commit()
            print("Existing data removed successfully")

    except Exception as e:
        print(f"Error clearing existing data: {str(e)}")
        conn.rollback()
        raise


def process_chunk(chunk, columns_map):
    """
    Process a chunk of data by applying column mappings and cleaning values.
    Handles missing values and converts them to None.

    Args:
        chunk (pd.DataFrame): DataFrame chunk to process
        columns_map (dict): Mapping of source columns to target columns

    Returns:
        pd.DataFrame: Processed chunk with cleaned data
    """
    processed_chunk = chunk[list(columns_map.keys())].copy()
    processed_chunk.rename(columns=columns_map, inplace=True)

    # Replace various forms of missing values with None
    missing_values = ['', ' ', '-999', 'NULL', 'PrivacySuppressed']
    processed_chunk.replace(missing_values, None, inplace=True)

    # Clean numeric columns by converting zeros and NaN to None
    numeric_columns = processed_chunk.select_dtypes(
        include=['float64', 'int64']).columns
    for col in numeric_columns:
        processed_chunk[col] = processed_chunk[col].apply(
            lambda x: None if pd.isna(x) or x == 0 else x
        )

    if 'OPEID' in processed_chunk.columns:
        processed_chunk = processed_chunk.dropna(subset=['OPEID'])

    return processed_chunk


def batch_insert(conn, table_name, df, batch_size=1000, pbar=None):
    """
    Insert data into database in batches with progress tracking.
    Uses transaction management to ensure data integrity.

    Args:
        conn (psycopg.Connection): Database connection
        table_name (str): Target table name
        df (pd.DataFrame): DataFrame containing data to insert
        batch_size (int): Number of rows per batch
        pbar (tqdm, optional): Progress bar object for tracking

    Raises:
        Exception: If batch insert fails
    """
    columns = df.columns.tolist()
    placeholders = ','.join(['%s'] * len(columns))
    insert_query = f"""
        INSERT INTO {table_name} ({','.join(columns)})
        VALUES ({placeholders})
    """

    with conn.cursor() as cur:
        try:
            conn.execute("BEGIN")

            for start_idx in range(0, len(df), batch_size):
                batch_df = df.iloc[start_idx:start_idx + batch_size]
                values = [tuple(row) for row in batch_df.values]

                try:
                    cur.executemany(insert_query, values)
                except Exception as e:
                    problem_rows = batch_df.index.tolist()
                    print(f"Error in rows {problem_rows[0]} to"
                          f"{problem_rows[-1]}")
                    print(f"Error details: {str(e)}")
                    raise

                if pbar:
                    pbar.update(len(batch_df))

            conn.commit()

        except Exception:
            print("Error during batch insert, rolling back")
            conn.rollback()
            raise
