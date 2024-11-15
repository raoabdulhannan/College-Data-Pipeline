import psycopg
import pandas as pd
import sys
from credentials import DB_NAME, DB_USER, DB_PASSWORD


def process_value(value, column_name=None):
    """Convert empty or invalid values to None."""
    if value in ("", "-999"):
        return None
    if column_name == "COUNTYCD" and value is not None:
        return str(value).zfill(5)
    return value


def load_ipeds(file_path):
    """Loads IPEDS csv into Institutions/Crosswalk DBs"""
    try:
        conn = psycopg.connect(
            host="pinniped.postgres.database.azure.com",
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()

        ipeds_df = pd.read_csv(file_path, encoding="windows-1252")

        inst_cols = {
            "UNITID": "UNITID", "INSTNM": "INSTNM", "ADDR": "ADDR",
            "CITY": "CITY", "STABBR": "STABBR", "ZIP": "ZIP",
            "LATITUDE": "LATITUDE", "LONGITUD": "LONGITUD",
            "CONTROL": "CONTROL", "OBEREG": "OBEREG", "CCBASIC": "CCBASIC",
            "CBSA": "CBSA", "CSA": "CSA", "COUNTYCD": "COUNTYCD"
        }

        cw_cols = ["UNITID", "OPEID"]

        inst_data = ipeds_df[inst_cols.keys()].applymap(
            lambda val: process_value(val)
        )

        for _, row in inst_data.iterrows():
            insert_query = f"""
                INSERT INTO InstitutionsTest2 ({', '.join(inst_cols.values())})
                VALUES ({', '.join(['%s'] * len(inst_cols))})
            """
            try:
                cur.execute(insert_query, tuple(row))
            except psycopg.errors.ForeignKeyViolation as e:
                print("Foreign key violation:", e)
                conn.rollback()
            except Exception as e:
                print("An error occurred:", e)
                conn.rollback()
            else:
                conn.commit()

        crosswalk_data = ipeds_df[cw_cols].map(process_value)
        for _, row in crosswalk_data.iterrows():
            insert_query = """
                INSERT INTO CrosswalksTest (UNITID, OPEID)
                VALUES (%s, %s)
            """
            try:
                cur.execute(insert_query, tuple(row))
            except psycopg.errors.ForeignKeyViolation as e:
                print("Foreign key violation:", e)
                conn.rollback()
            except Exception as e:
                print("An error occurred:", e)
                conn.rollback()
            else:
                conn.commit()

        print("Data loaded with no errors")

    except Exception as e:
        print("Database connection error:", e)

    cur.close()
    conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Script needs csv file as command line arg")
        sys.exit(1)

    file_path = sys.argv[1]
    load_ipeds(file_path)
