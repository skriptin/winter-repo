import sqlite3 as sql
import pandas as pd
import json
import os
import contextlib
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000) 
pd.set_option('display.max_colwidth', None) 

def create_pandas_df(conn):
    """
    Creates a wide-format Pandas DataFrame from the database.
    Each row represents a report and each column a parameter.
    """
    sql_query = "SELECT report_id, parameter_id, value FROM main"
    long_df = pd.read_sql_query(sql_query, conn)

    if long_df.empty:
        return pd.DataFrame()

    wide_df = long_df.pivot(index='report_id', columns='parameter_id', values='value')
    wide_df = wide_df.reset_index()
    wide_df.columns.name = None

    def cast_value(val):
        if isinstance(val, str):
            try:
                # Attempt to load as JSON, which handles lists/dicts
                return json.loads(val)
            except json.JSONDecodeError:
                # If it's not JSON, it's just a regular string
                return val
        # Return all other types (numbers, None, etc.) as they are
        return val

    for col in wide_df.columns:
        if col != 'report_id':
            # First, cast JSON strings back to Python objects (e.g., lists)
            series = wide_df[col].map(cast_value)
            
            # Next, convert strings that represent numbers into numeric types.
            # The 'ignore' flag ensures that actual strings or lists are not affected.
            wide_df[col] = pd.to_numeric(series, errors='ignore')

    return wide_df




# --- Main Execution ---
print("Starting script...")
# Define paths
script_dir = os.path.dirname(os.path.abspath(__file__))
db_dir = os.path.join(script_dir, '..', 'db') 
db_pth = os.path.join(db_dir, 'database.db')
reports_dir = os.path.join(script_dir, '..', 'reports')
report_file_path = os.path.join(reports_dir, 'analysis_report.txt')

# Create the reports directory if it doesn't exist
os.makedirs(reports_dir, exist_ok=True)

conn = sql.connect(db_pth)
print("Connected to db.")
master_data = create_pandas_df(conn)

if not master_data.empty:
    print(f"DataFrame created successfully with {len(master_data)} rows.")
    print(f"Generating report at: {report_file_path}")

    # Open the report file and redirect all print statements to it
    with open(report_file_path, 'w', encoding='utf-8') as f:
        with contextlib.redirect_stdout(f):
            
            # ===================================================================
            # START OF ANALYSIS SCRIPT (ALL OUTPUT GOES TO FILE)
            # ===================================================================

            print(f"Analysis Report for {len(master_data)} Mine Studies\n")

            print("ANALYSIS: MINE TYPES SUMMARY\n" + "="*30)
            if 'mine_type' in master_data.columns:
                print("Counts for each unique mine type:")
                print(master_data['mine_type'].value_counts())
            else:
                print("Warning: 'mine_type' column not found.")

            print("\nANALYSIS: DEPOSIT TYPES\n" + "="*30)
            deposit_col = 'deposit_type'
            if deposit_col in master_data.columns and master_data[deposit_col].notna().any():
                print("Counts for each unique deposit type:")
                print(master_data[deposit_col].value_counts())
            else:
                print("No data available for deposit type.")
            
            print("\nANALYSIS: COMMODITY PRODUCTION & LOCATION\n" + "="*45)
            commodities_to_check = ['copper', 'lead', 'zinc', 'iron', 'gold', 'silver', 'sulphur']
            for commodity in commodities_to_check:
                produces_col = f'produces_{commodity}'
                if produces_col in master_data.columns:
                    producers_df = master_data[master_data[produces_col] == True]
                    num_producers = len(producers_df)
                    print(f"\n--- {commodity.title()} Production Summary ---")
                    print(f"Total mines producing {commodity.title()}: {num_producers}")
                    if num_producers > 0 and 'country' in producers_df.columns:
                        print("Location breakdown:")
                        print(producers_df['country'].value_counts())
                else:
                    print(f"\n--- {commodity.title()} Production Summary ---")
                    print(f"Warning: Column '{produces_col}' not found.")
                    
            print("\nANALYSIS: OPEN PIT MINING COSTS\n" + "="*35)
            cost_cols_to_analyze = [
                'open_pit_mining_cost_dollars_per_t_milled_or_processed',
                'open_pit_mining_cost_dollars_per_t_mined_or_moved'
            ]
            for col in cost_cols_to_analyze:
                print(f"\n--- Statistics for '{col}' ---")
                if col in master_data.columns and master_data[col].notna().any():
                    print(master_data[col].describe())
                else:
                    print("No data available for this parameter.")
                    
            print("\n--- Summary for 'open_pit_mining_cost_currency' ---")
            currency_col = 'open_pit_mining_cost_currency'
            if currency_col in master_data.columns and master_data[currency_col].notna().any():
                print(master_data[currency_col].value_counts())
            else:
                print("No data available for this parameter.")
            
            # ===================================================================
            # END OF ANALYSIS SCRIPT
            # ===================================================================

    print("Report generated successfully.")
else:
    print("Master DataFrame is empty. No analysis to perform.")

conn.close()
print("Database connection closed. Script finished.")