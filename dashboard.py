import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
from credentials import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST

# Define US regions
US_REGIONS = {
    'Northeast': ['ME', 'NH', 'VT', 'MA', 'RI', 'CT', 'NY', 'NJ', 'PA'],
    'Midwest': ['OH', 'MI', 'IN', 'IL', 'WI', 'MN', 'IA', 'MO', 'ND', 'SD', 'NE', 'KS'],
    'South': ['DE', 'MD', 'DC', 'VA', 'WV', 'NC', 'SC', 'GA', 'FL', 'KY', 'TN', 'AL', 'MS', 'AR', 'LA', 'OK', 'TX'],
    'West': ['MT', 'ID', 'WY', 'CO', 'NM', 'AZ', 'UT', 'NV', 'WA', 'OR', 'CA', 'AK', 'HI']
}

# Database connection function
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

# Streamlit Dashboard
def main():
    st.set_page_config(page_title="College Scorecard Dashboard", layout="wide")
    st.title("College Scorecard Dashboard (2022)")
    st.markdown("""
        This dashboard provides insights into tuition trends, Pell Grant recipients, 
        3-year loan default rates, and other institutional metrics.
    """)

    # Sidebar Filters
    st.sidebar.header("Filters")
    view_type = st.sidebar.radio("Select View Type", ["US Regions", "States"])

    # Get available states
    conn = connect_to_db()
    if conn:
        state_query = "SELECT DISTINCT UPPER(TRIM(stabbr)) AS state FROM institutions ORDER BY state;"
        states = pd.read_sql_query(state_query, conn)['state'].tolist()
        conn.close()
    else:
        states = []

    if view_type == "States":
        selected_states = st.sidebar.multiselect("Select States", states, default=states[:5])
        if not selected_states:
            st.warning("Please select at least one state to view data.")
            return
    else:
        selected_states = states

    # Section 1: Tuition Trends
    st.header(f"Tuition Trends ({'All US Regions' if view_type == 'US Regions' else ', '.join(selected_states)})")
    tuition_query = f"""
        SELECT UPPER(stabbr) AS state, AVG(tuitionfee_in::NUMERIC) AS avg_in_state_tuition, 
               AVG(tuitionfee_out::NUMERIC) AS avg_out_state_tuition
        FROM financial_data
        JOIN institutions ON financial_data.unitid = institutions.unitid
        WHERE year = 2022
        {"AND UPPER(stabbr) IN %(selected_states)s" if view_type == "States" else ""}
        GROUP BY stabbr;
    """
    tuition_df = pd.read_sql_query(tuition_query, connect_to_db(), params={"selected_states": tuple(selected_states)})

    if view_type == "US Regions":
        tuition_df['region'] = tuition_df['state'].map(
            lambda s: next((region for region, states in US_REGIONS.items() if s.upper() in states), "Other")
        )
        tuition_summary = tuition_df.groupby('region')[['avg_in_state_tuition', 'avg_out_state_tuition']].mean().reset_index()
    else:
        tuition_summary = tuition_df[tuition_df['state'].isin(selected_states)]

    if not tuition_summary.empty:
        st.dataframe(tuition_summary)
        tuition_fig = px.bar(
            tuition_summary,
            x='region' if view_type == "US Regions" else 'state',
            y=['avg_in_state_tuition', 'avg_out_state_tuition'],
            title='Average Tuition by State/Region',
            labels={'value': 'Tuition ($)', 'variable': 'Tuition Type'},
            barmode='group'
        )
        st.plotly_chart(tuition_fig, use_container_width=True)
    else:
        st.warning("No tuition data available for the selected criteria.")

    # Section 2: Pell Grant Recipients
    st.header(f"Pell Grant Recipients ({'All US Regions' if view_type == 'US Regions' else ', '.join(selected_states)})")
    pctpell_query = f"""
        SELECT UPPER(stabbr) AS state, AVG(pctpell::NUMERIC) AS avg_pct_pell
        FROM financial_data
        JOIN institutions ON financial_data.unitid = institutions.unitid
        WHERE year = 2022
        {"AND UPPER(stabbr) IN %(selected_states)s" if view_type == "States" else ""}
        GROUP BY stabbr;
    """
    pctpell_df = pd.read_sql_query(pctpell_query, connect_to_db(), params={"selected_states": tuple(selected_states)})

    if view_type == "US Regions":
        pctpell_df['region'] = pctpell_df['state'].map(
            lambda s: next((region for region, states in US_REGIONS.items() if s.upper() in states), "Other")
        )
        pctpell_summary = pctpell_df.groupby('region')['avg_pct_pell'].mean().reset_index()
    else:
        pctpell_summary = pctpell_df[pctpell_df['state'].isin(selected_states)]

    if not pctpell_summary.empty:
        st.dataframe(pctpell_summary)
        pctpell_fig = px.bar(
            pctpell_summary,
            x='region' if view_type == "US Regions" else 'state',
            y='avg_pct_pell',
            title='Average Percentage of Pell Grant Recipients by State/Region',
            labels={'avg_pct_pell': 'Pell Grant Recipients (%)'},
            color='avg_pct_pell'
        )
        st.plotly_chart(pctpell_fig, use_container_width=True)
    else:
        st.warning("No Pell Grant data available for the selected criteria.")

    # Section 3: 3-Year Loan Default Rates (CDR3)
    st.header(f"3-Year Loan Default Rates ({'All US Regions' if view_type == 'US Regions' else ', '.join(selected_states)})")
    cdr3_query = f"""
        SELECT UPPER(stabbr) AS state, AVG(cdr3::NUMERIC) AS avg_cdr3
        FROM financial_data
        JOIN institutions ON financial_data.unitid = institutions.unitid
        WHERE year = 2022
        {"AND UPPER(stabbr) IN %(selected_states)s" if view_type == "States" else ""}
        GROUP BY stabbr;
    """
    cdr3_df = pd.read_sql_query(cdr3_query, connect_to_db(), params={"selected_states": tuple(selected_states)})

    if view_type == "US Regions":
        cdr3_df['region'] = cdr3_df['state'].map(
            lambda s: next((region for region, states in US_REGIONS.items() if s.upper() in states), "Other")
        )
        cdr3_summary = cdr3_df.groupby('region')['avg_cdr3'].mean().reset_index()
    else:
        cdr3_summary = cdr3_df[cdr3_df['state'].isin(selected_states)]

    if not cdr3_summary.empty:
        st.dataframe(cdr3_summary)
        cdr3_fig = px.bar(
            cdr3_summary,
            x='region' if view_type == "US Regions" else 'state',
            y='avg_cdr3',
            title='Average 3-Year Cohort Default Rates by State/Region',
            labels={'avg_cdr3': 'Default Rate (%)'},
            color='avg_cdr3'
        )
        st.plotly_chart(cdr3_fig, use_container_width=True)
    else:
        st.warning("No default rate data available for the selected criteria.")

    st.text("Dashboard created by Team Rhodes, 2024.")

if __name__ == "__main__":
    main()
