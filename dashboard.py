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

def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

def main():
    st.set_page_config(page_title="College Scorecard Dashboard", layout="wide")
    st.title("College Scorecard Dashboard")
    st.markdown("""
        This dashboard provides insights into tuition trends, admission rates, 
        Pell Grant recipients, and 3-year loan default rates.
    """)

    # Sidebar Filters
    st.sidebar.header("Filters")
    view_type = st.sidebar.radio("Select View Type", ["US Regions", "States"])
    
    selected_year = st.sidebar.selectbox(
        "Select Year",
        range(2019, 2023),
        index=0
    )

    # Get available states
    conn = connect_to_db()
    if conn:
        state_query = """
            SELECT DISTINCT UPPER(TRIM(stabbr)) AS state 
            FROM institutions 
            ORDER BY UPPER(TRIM(stabbr));
        """
        states = pd.read_sql_query(state_query, conn)['state'].tolist()
        
        if view_type == "States":
            selected_states = st.sidebar.multiselect("Select States", states, default=states[:5])
            if not selected_states:
                st.warning("Please select at least one state to view data.")
                return
        else:
            selected_states = states

        # Section 1: Admission Rates
        st.header(f"Admission Rates {selected_year}")
        
        admission_query = f"""
        SELECT 
            UPPER(i.stabbr) AS state, 
            COUNT(*) as total_institutions,
            COUNT(csa.adm_rate) as institutions_with_data,
            AVG(CASE 
                WHEN csa.adm_rate BETWEEN 0 AND 1 
                THEN csa.adm_rate * 100
                END) AS avg_admission_rate
        FROM institutions i
        LEFT JOIN college_scorecard_annual csa 
            ON i.unitid = csa.unitid 
            AND csa.year = %(year)s
        WHERE i.stabbr IS NOT NULL
        {"AND UPPER(i.stabbr) IN %(selected_states)s" if view_type == "States" else ""}
        GROUP BY i.stabbr
        HAVING AVG(CASE WHEN csa.adm_rate BETWEEN 0 AND 1 THEN csa.adm_rate * 100 END) IS NOT NULL;
        """
        
        admission_df = pd.read_sql_query(
            admission_query, 
            conn, 
            params={"selected_states": tuple(selected_states), "year": selected_year}
        )

        if not admission_df.empty:
            if view_type == "US Regions":
                admission_df['region'] = admission_df['state'].map(
                    lambda s: next((region for region, states in US_REGIONS.items() if s in states), "Other")
                )
                admission_summary = admission_df.groupby('region').agg({
                    'avg_admission_rate': 'mean',
                    'total_institutions': 'sum',
                    'institutions_with_data': 'sum'
                }).reset_index()
            else:
                admission_summary = admission_df

            admission_summary = admission_summary.sort_values('avg_admission_rate', ascending=True)
            
            st.markdown(f"""
            ### Data Coverage
            - Total institutions: {admission_summary['total_institutions'].sum():,}
            - Institutions with valid admission rate data: {admission_summary['institutions_with_data'].sum():,}
            - Data coverage: {(admission_summary['institutions_with_data'].sum() / admission_summary['total_institutions'].sum() * 100):.1f}%
            """)

            # Display the data table
            st.write("### Admission Rate Data")
            st.dataframe(admission_summary)

            fig = px.bar(
                admission_summary,
                x='region' if view_type == "US Regions" else 'state',
                y='avg_admission_rate',
                title=f'Average Admission Rates by {view_type[:-1]} ({selected_year})',
                labels={
                    'avg_admission_rate': 'Admission Rate (%)',
                    'region': 'Region',
                    'state': 'State'
                },
                color='avg_admission_rate',
                color_continuous_scale='RdYlBu_r'
            )
            fig.update_layout(
                xaxis_tickangle=-45,
                yaxis_range=[0, 100],
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)

        # Section 2: Tuition Trends
        st.header(f"Tuition Trends {selected_year}")
        
        tuition_query = f"""
        SELECT 
            UPPER(i.stabbr) AS state,
            f.tuitionfee_in,
            f.tuitionfee_out,
            1 as institution_count
        FROM institutions i
        JOIN crosswalks c ON i.unitid = c.unitid
        JOIN financial_data f ON c.opeid = f.opeid AND f.year = %(year)s
        WHERE i.stabbr IS NOT NULL
        {"AND UPPER(i.stabbr) IN %(selected_states)s" if view_type == "States" else ""};
        """
        
        tuition_df = pd.read_sql_query(
            tuition_query,
            conn,
            params={"selected_states": tuple(selected_states), "year": selected_year}
        )

        if not tuition_df.empty:
            if view_type == "US Regions":
                # Map states to regions
                tuition_df['region'] = tuition_df['state'].map(
                    lambda x: next((region for region, states in US_REGIONS.items() if x in states), "Other")
                )
                
                # Aggregate by region
                tuition_summary = tuition_df.groupby('region').agg({
                    'tuitionfee_in': 'mean',
                    'tuitionfee_out': 'mean',
                    'institution_count': 'sum'
                }).reset_index()
                
                # Rename columns
                tuition_summary = tuition_summary.rename(columns={
                    'tuitionfee_in': 'avg_in_state_tuition',
                    'tuitionfee_out': 'avg_out_state_tuition',
                    'institution_count': 'total_institutions'
                })
            else:
                # For state view, aggregate by state
                tuition_summary = tuition_df.groupby('state').agg({
                    'tuitionfee_in': 'mean',
                    'tuitionfee_out': 'mean',
                    'institution_count': 'sum'
                }).reset_index()
                
                tuition_summary = tuition_summary.rename(columns={
                    'tuitionfee_in': 'avg_in_state_tuition',
                    'tuitionfee_out': 'avg_out_state_tuition',
                    'institution_count': 'total_institutions'
                })

            tuition_summary = tuition_summary.sort_values('avg_in_state_tuition', ascending=True)

            # Add institutions with data counts
            tuition_summary['institutions_with_in_state'] = tuition_summary['total_institutions']
            tuition_summary['institutions_with_out_state'] = tuition_summary['total_institutions']

            st.markdown(f"""
            ### Data Coverage
            - Total institutions: {tuition_summary['total_institutions'].sum():,}
            - Institutions with tuition data: {tuition_summary['total_institutions'].sum():,}
            """)

            # Display the data table
            st.write("### Tuition Data")
            st.dataframe(tuition_summary)

            # Reshape data for grouped bar chart
            tuition_melted = pd.melt(
                tuition_summary,
                id_vars=['region' if view_type == "US Regions" else 'state'],
                value_vars=['avg_in_state_tuition', 'avg_out_state_tuition'],
                var_name='Tuition Type',
                value_name='Amount'
            )

            tuition_melted['Tuition Type'] = tuition_melted['Tuition Type'].map({
                'avg_in_state_tuition': 'In-State',
                'avg_out_state_tuition': 'Out-of-State'
            })

            fig = px.bar(
                tuition_melted,
                x='region' if view_type == "US Regions" else 'state',
                y='Amount',
                color='Tuition Type',
                barmode='group',
                title=f'Average Tuition by {view_type[:-1]} ({selected_year})',
                labels={
                    'Amount': 'Tuition Cost ($)',
                    'region': 'Region',
                    'state': 'State'
                },
                color_discrete_sequence=['rgb(99,110,250)', 'rgb(239,85,59)']
            )
            fig.update_layout(
                xaxis_tickangle=-45,
                showlegend=True
            )
            fig.update_traces(
                hovertemplate='%{y:$,.0f}'
            )
            st.plotly_chart(fig, use_container_width=True)

        # Section 3: Pell Grant Recipients
        st.header(f"Pell Grant Recipients {selected_year}")
        
        pell_query = f"""
        SELECT 
            UPPER(i.stabbr) AS state,
            f.pctpell * 100 as pell_pct,
            1 as institution_count
        FROM institutions i
        JOIN crosswalks c ON i.unitid = c.unitid
        JOIN financial_data f ON c.opeid = f.opeid AND f.year = %(year)s
        WHERE i.stabbr IS NOT NULL
        {"AND UPPER(i.stabbr) IN %(selected_states)s" if view_type == "States" else ""};
        """
        
        pell_df = pd.read_sql_query(
            pell_query,
            conn,
            params={"selected_states": tuple(selected_states), "year": selected_year}
        )

        if not pell_df.empty:
            if view_type == "US Regions":
                pell_df['region'] = pell_df['state'].map(
                    lambda x: next((region for region, states in US_REGIONS.items() if x in states), "Other")
                )
                pell_summary = pell_df.groupby('region').agg({
                    'pell_pct': 'mean',
                    'institution_count': 'sum'
                }).reset_index()
                pell_summary = pell_summary.rename(columns={
                    'pell_pct': 'avg_pct_pell',
                    'institution_count': 'total_institutions'
                })
            else:
                pell_summary = pell_df.groupby('state').agg({
                    'pell_pct': 'mean',
                    'institution_count': 'sum'
                }).reset_index()
                pell_summary = pell_summary.rename(columns={
                    'pell_pct': 'avg_pct_pell',
                    'institution_count': 'total_institutions'
                })

            pell_summary = pell_summary.sort_values('avg_pct_pell', ascending=False)
            pell_summary['institutions_with_data'] = pell_summary['total_institutions']

            st.markdown(f"""
            ### Data Coverage
            - Total institutions: {pell_summary['total_institutions'].sum():,}
            - Institutions with Pell Grant data: {pell_summary['total_institutions'].sum():,}
            """)

            # Display the data table
            st.write("### Pell Grant Data")
            st.dataframe(pell_summary)

            fig = px.bar(
                pell_summary,
                x='region' if view_type == "US Regions" else 'state',
                y='avg_pct_pell',
                title=f'Average Percentage of Pell Grant Recipients by {view_type[:-1]} ({selected_year})',
                labels={
                    'avg_pct_pell': 'Pell Grant Recipients (%)',
                    'region': 'Region',
                    'state': 'State'
                },
                color='avg_pct_pell',
                color_continuous_scale='Viridis'
            )
            fig.update_layout(
                xaxis_tickangle=-45,
                yaxis_range=[0, 100],
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)

        # Section 4: Default Rates
        st.header(f"3-Year Loan Default Rates {selected_year}")
        
        default_query = f"""
        SELECT 
            UPPER(i.stabbr) AS state,
            f.cdr3 as default_rate,
            1 as institution_count
        FROM institutions i
        JOIN crosswalks c ON i.unitid = c.unitid
        JOIN financial_data f ON c.opeid = f.opeid AND f.year = %(year)s
        WHERE i.stabbr IS NOT NULL
        {"AND UPPER(i.stabbr) IN %(selected_states)s" if view_type == "States" else ""};
        """
        
        default_df = pd.read_sql_query(
            default_query,
            conn,
            params={"selected_states": tuple(selected_states), "year": selected_year}
        )

        if not default_df.empty:
            if view_type == "US Regions":
                default_df['region'] = default_df['state'].map(
                    lambda x: next((region for region, states in US_REGIONS.items() if x in states), "Other")
                )
                default_summary = default_df.groupby('region').agg({
                    'default_rate': 'mean',
                    'institution_count': 'sum'
                }).reset_index()
                default_summary = default_summary.rename(columns={
                    'default_rate': 'avg_default_rate',
                    'institution_count': 'total_institutions'
                })
            else:
                default_summary = default_df.groupby('state').agg({
                    'default_rate': 'mean',
                    'institution_count': 'sum'
                }).reset_index()
                default_summary = default_summary.rename(columns={
                    'default_rate': 'avg_default_rate',
                    'institution_count': 'total_institutions'
                })

            default_summary = default_summary.sort_values('avg_default_rate', ascending=False)
            default_summary['institutions_with_data'] = default_summary['total_institutions']

            st.markdown(f"""
            ### Data Coverage
            - Total institutions: {default_summary['total_institutions'].sum():,}
            - Institutions with default rate data: {default_summary['total_institutions'].sum():,}
            """)

            # Display the data table
            st.write("### Default Rate Data")
            st.dataframe(default_summary)

            fig = px.bar(
                default_summary,
                x='region' if view_type == "US Regions" else 'state',
                y='avg_default_rate',
                title=f'Average 3-Year Default Rates by {view_type[:-1]} ({selected_year})',
                labels={
                    'avg_default_rate': 'Default Rate (%)',
                    'region': 'Region',
                    'state': 'State'
                },
                color='avg_default_rate',
                color_continuous_scale='RdYlBu'
            )
            fig.update_layout(
                xaxis_tickangle=-45,
                yaxis_range=[0, max(default_summary['avg_default_rate']) * 1.1],
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)

        # Section 5: Median Earnings
        st.header(f"Median Earnings 8 Years After Entry {selected_year}")
        
        earnings_query = f"""
        SELECT 
            UPPER(i.stabbr) AS state,
            f.md_earn_wne_p8 as earnings,
            1 as institution_count
        FROM institutions i
        JOIN crosswalks c ON i.unitid = c.unitid
        JOIN financial_data f ON c.opeid = f.opeid AND f.year = %(year)s
        WHERE i.stabbr IS NOT NULL
        {"AND UPPER(i.stabbr) IN %(selected_states)s" if view_type == "States" else ""};
        """
        
        earnings_df = pd.read_sql_query(
            earnings_query,
            conn,
            params={"selected_states": tuple(selected_states), "year": selected_year}
        )

        if not earnings_df.empty:
            if view_type == "US Regions":
                earnings_df['region'] = earnings_df['state'].map(
                    lambda x: next((region for region, states in US_REGIONS.items() if x in states), "Other")
                )
                earnings_summary = earnings_df.groupby('region').agg({
                    'earnings': 'mean',
                    'institution_count': 'sum'
                }).reset_index()
                earnings_summary = earnings_summary.rename(columns={
                    'earnings': 'median_earnings',
                    'institution_count': 'total_institutions'
                })
            else:
                earnings_summary = earnings_df.groupby('state').agg({
                    'earnings': 'mean',
                    'institution_count': 'sum'
                }).reset_index()
                earnings_summary = earnings_summary.rename(columns={
                    'earnings': 'median_earnings',
                    'institution_count': 'total_institutions'
                })

            earnings_summary = earnings_summary.sort_values('median_earnings', ascending=False)
            earnings_summary['institutions_with_data'] = earnings_summary['total_institutions']

            st.markdown(f"""
            ### Data Coverage
            - Total institutions: {earnings_summary['total_institutions'].sum():,}
            - Institutions with earnings data: {earnings_summary['total_institutions'].sum():,}
            """)

            # Display the data table
            st.write("### Median Earnings Data")
            st.dataframe(earnings_summary)

            fig = px.bar(
                earnings_summary,
                x='region' if view_type == "US Regions" else 'state',
                y='median_earnings',
                title=f'Average Median Earnings by {view_type[:-1]} ({selected_year})',
                labels={
                    'median_earnings': 'Median Earnings ($)',
                    'region': 'Region',
                    'state': 'State'
                },
                color='median_earnings',
                color_continuous_scale='Viridis'
            )
            fig.update_layout(
                xaxis_tickangle=-45,
                showlegend=False
            )
            fig.update_traces(
                hovertemplate='%{y:$,.0f}'
            )
            st.plotly_chart(fig, use_container_width=True)

    if conn:
        conn.close()

    st.text("Dashboard created by Team Rhodes, 2024.")

if __name__ == "__main__":
    main()