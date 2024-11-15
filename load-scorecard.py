import psycopg
import pandas as pd
import sys
from credentials import DB_NAME, DB_USER, DB_PASSWORD

DB_HOST = "pinniped.postgres.database.azure.com"


def process_value(value, column_name=None):
    """Convert empty or invalid values to None and format currency."""
    if pd.isnull(value) or value in ("", "-999"):
        return None
    if column_name in {"AVGFACSAL", "TUITIONFEE_IN", "TUITIONFEE_OUT",
                       "TUITIONFEE_PROG", "NPT4_PUB", "DEBT_MDN",
                       "MD_EARN_WNE_P8"} and value is not None:
        # Format as currency string for PostgreSQL MONEY type
        return f"${float(value):,.2f}"
    return value


def load_data(file_path):
    """Loads college scoreboard data into schema tables."""
    try:
        conn = psycopg.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()

        df = pd.read_csv(file_path, encoding="windows-1252")
        df["YEAR"] = 2022

        sb_cols = {
            "UNITID": "UNITID", "YEAR": "YEAR", "ACCREDAGENCY": "ACCREDAGENCY",
            "PREDDEG": "PREDDEG", "HIGHDEG": "HIGHDEG", "ADM_RATE": "ADM_RATE",
            "C150_4": "C150_4", "C200_4": "C200_4", "AVGFACSAL": "AVGFACSAL"
        }

        scoreboard_data = df[sb_cols.keys()].apply(
            lambda row: [process_value(row[col], col) for col in sb_cols.keys()],
            axis=1
        )

        for row in scoreboard_data:
            insert_query = f"""
                INSERT INTO College_SB_AnnualT4 ({', '.join(sb_cols.values())})
                VALUES ({', '.join(['%s'] * len(sb_cols))})
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

        fin_cols = {
            "OPEID": "OPEID", "UNITID": "UNITID", "YEAR": "YEAR",
            "TUITIONFEE_IN": "TUITIONFEE_IN", "TUITIONFEE_OUT": "TUITIONFEE_OUT",
            "TUITIONFEE_PROG": "TUITIONFEE_PROG", "NPT4_PUB": "NPT4_PUB",
            "PCTPELL": "PCTPELL", "DEBT_MDN": "DEBT_MDN", "RPY_3YR_RT": "RPY_3YR_RT",
            "CDR2": "CDR2", "CDR3": "CDR3", "MD_EARN_WNE_P8": "MD_EARN_WNE_P8"
        }

        financial_data = df[fin_cols.keys()].apply(
            lambda row: [process_value(row[col], col) for col in fin_cols.keys()],
            axis=1
        )

        for row in financial_data:
            insert_query = f"""
                INSERT INTO Financial_DataTest4 ({', '.join(fin_cols.values())})
                VALUES ({', '.join(['%s'] * len(fin_cols))})
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
    load_data(file_path)
