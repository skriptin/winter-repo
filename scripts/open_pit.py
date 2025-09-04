import sqlite3 as sql
import pandas as pd
import json
import os
import contextlib
import numpy as np
import re
import matplotlib.pyplot as plt
import seaborn as sns
# --- PANDAS DISPLAY OPTIONS ---
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
                return json.loads(val)
            except (json.JSONDecodeError, TypeError):
                return val
        return val

    for col in wide_df.columns:
        if col != 'report_id':
            series = wide_df[col].map(cast_value)
            # Using 'ignore' is safer for initial load to preserve text/lists.
            # We will coerce specific columns to numeric later.
            wide_df[col] = pd.to_numeric(series, errors='ignore')

    return wide_df

def process_and_analyze_deposits(df, report_file_handle):
    """
    Sub-function to intelligently process, collapse, and atomize deposit types.
    """
    deposit_col = 'deposit_type'
    if deposit_col not in df.columns or df[deposit_col].isna().all():
        print("No data available for deposit type analysis.")
        return df

    processed_df = df.copy()
    
    def clean_entry(entry):
        if not isinstance(entry, list):
            entry = [str(entry)]
        cleaned_list = []
        for item in entry:
            clean_item = re.sub(r'\s*\(.*?\)', '', str(item)).strip()
            if clean_item:
                cleaned_list.append(clean_item)
        return cleaned_list

    processed_df['cleaned_deposits'] = processed_df[deposit_col].apply(clean_entry)

    def create_combination_key(cleaned_list):
        if not cleaned_list: return "Unknown"
        return ", ".join(sorted(cleaned_list))

    processed_df['deposit_combination'] = processed_df['cleaned_deposits'].apply(create_combination_key)
    
    print("\n--- Analysis 1: Counts of Unique Deposit TYPE COMBINATIONS (Collapsed) ---", file=report_file_handle)
    print(processed_df['deposit_combination'].value_counts(), file=report_file_handle)

    exploded_df = processed_df.explode('cleaned_deposits')
    exploded_df.rename(columns={'cleaned_deposits': 'atomized_deposit_type'}, inplace=True)
    
    print("\n--- Analysis 2: Total Occurrences of Each INDIVIDUAL Deposit Type (Atomized) ---", file=report_file_handle)
    print(exploded_df['atomized_deposit_type'].value_counts(), file=report_file_handle)

    return exploded_df

# --- Main Execution ---
print("Starting script...")
script_dir = os.path.dirname(os.path.abspath(__file__))
db_dir = os.path.join(script_dir, '..', 'db') 
db_pth = os.path.join(db_dir, 'database.db')
reports_dir = os.path.join(script_dir, '..', 'reports')
report_file_path = os.path.join(reports_dir, 'openpit_report.txt')

os.makedirs(reports_dir, exist_ok=True)

conn = sql.connect(db_pth)
print("Connected to db.")
master_data = create_pandas_df(conn)

if not master_data.empty:
    if 'mine_type' in master_data.columns:
        master_data['mine_type'] = master_data['mine_type'].astype(str)
        open_pit_df = master_data[master_data['mine_type'].str.contains("Open Pit", na=False, case=False)].copy()
        print(f"Found {len(open_pit_df)} reports related to Open Pit mining.")
    else:
        print("Warning: 'mine_type' column not found.")
        open_pit_df = pd.DataFrame()

    if not open_pit_df.empty:
        print(f"Generating Open Pit analysis report at: {report_file_path}")

        # --- Feature Engineering & Data Type Coercion ---
        if 'total_material_mined' in open_pit_df and 'life_of_mine' in open_pit_df:
            # Coerce inputs to numeric before calculation
            total_material = pd.to_numeric(open_pit_df['total_material_mined'], errors='coerce')
            lom = pd.to_numeric(open_pit_df['life_of_mine'], errors='coerce')
            lom_in_days = lom * 365.25
            open_pit_df['calculated_mining_rate_tpd'] = total_material / lom_in_days.replace(0, np.nan)

        if 'effective_date' in open_pit_df:
            open_pit_df['year'] = pd.to_datetime(open_pit_df['effective_date'], errors='coerce').dt.year

        # ===================================================================
        # --- NEW SECTION: Force numeric types for key analysis columns ---
        # ===================================================================
        cols_to_make_numeric = [
            'stripping_ratio', 'open_pit_mining_cost_dollars_per_t_mined_or_moved',
            'total_operating_cost_dollars_per_t_milled', 'initial_capex_in_millions',
            'life_of_mine', 'processing_rate', 'total_ore_mined', 'total_waste_mined',
            'copper_price', 'gold_price', 'silver_price', 'copper_cut_off_grade',
            'gold_cut_off_grade', 'copper_metallurgical_recovery', 'gold_metallurgical_recovery',
            'pre_tax_npv_8_in_millions', 'after_tax_irr'
        ]
        for col in cols_to_make_numeric:
            if col in open_pit_df.columns:
                open_pit_df[col] = pd.to_numeric(open_pit_df[col], errors='coerce')
        # ===================================================================

        with open(report_file_path, 'w', encoding='utf-8') as f:
            print(f"\nThis report is based on {len(open_pit_df)} studies with an open pit component.", file=f)

            print("\n" + "="*20 + " ADVANCED DEPOSIT TYPE ANALYSIS " + "="*20, file=f)
            exploded_deposit_df = process_and_analyze_deposits(open_pit_df, f)

            
            cost_col = 'open_pit_mining_cost_dollars_per_t_mined_or_moved'
            
            if 'atomized_deposit_type' in exploded_deposit_df.columns and cost_col in exploded_deposit_df.columns:
                # Ensure the cost column is numeric before processing
                exploded_deposit_df[cost_col] = pd.to_numeric(exploded_deposit_df[cost_col], errors='coerce')

                # Step 1: Calculate the descriptive statistics for all groups
                hardness_proxy = exploded_deposit_df.groupby('atomized_deposit_type')[cost_col].describe()
                
                # Step 2 & 3: Filter for groups with sufficient data (count >= 10) and sort by the mean
                final_hardness_proxy = (hardness_proxy[hardness_proxy['count'] >= 10]
                                        .sort_values(by='mean', ascending=False))
                
                print(final_hardness_proxy, file=f)
            else:
                print("Could not perform hardness analysis: 'deposit_type' or mining cost column is missing.", file=f)

            if 'year' in open_pit_df and 'initial_capex_in_millions' in open_pit_df:
                print("\n--- Initial Capex (in Millions) by Year ---", file=f)
                print(open_pit_df.groupby('year')['initial_capex_in_millions'].describe(), file=f)
            else:
                print("Could not perform Capex trend analysis: 'year' or 'initial_capex_in_millions' column is missing.", file=f)

            print("\n\n" + "="*20 + " DIRECT FACTOR ANALYSIS " + "="*20, file=f)
            
            print("\n--- FAF 1: Country Distribution ---", file=f)
            if 'country' in open_pit_df.columns:
                print(open_pit_df['country'].value_counts(), file=f)
            
            print("\n--- FAF 22: Open Pit Mining Rate (Calculated) ---", file=f)
            if 'calculated_mining_rate_tpd' in open_pit_df:
                print("Statistics for Calculated Mining Rate (tonnes per day):", file=f)
                print(open_pit_df['calculated_mining_rate_tpd'].describe(), file=f)
            else:
                print("Could not calculate mining rate.", file=f)
            
            print("\n--- FAF 23: Average Strip Ratio (Direct) ---", file=f)
            if 'stripping_ratio' in open_pit_df:
                print(open_pit_df['stripping_ratio'].describe(), file=f)
            else:
                print("No data for 'stripping_ratio'.", file=f)
            
            print("\n--- Total OP Mining Cost (Direct) ---", file=f)
            if cost_col in open_pit_df:
                print(f"Statistics for '{cost_col}':", file=f)
                print(open_pit_df[cost_col].describe(), file=f)
            else:
                print("No data for total open pit mining cost.", file=f)

           # ===================================================================
            # --- NEW VISUALIZATION SECTION ---
            # ===================================================================
            print("\n\n" + "="*20 + " DATA VISUALIZATIONS " + "="*20, file=f)
            print("The following plots have been generated and saved to the 'reports' directory.", file=f)
            
            sns.set_theme(style="whitegrid")
            
            plot_col = 'calculated_mining_rate_tpd'
            if plot_col in open_pit_df.columns and open_pit_df[plot_col].notna().any():
                plot_data = open_pit_df[plot_col].dropna()

                # --- Histogram ---
                plt.figure(figsize=(12, 7))
                sns.histplot(plot_data, log_scale=True, kde=True, bins=50)
                plt.title('Distribution of Calculated Mining Rate (Log Scale)', fontsize=16)
                plt.xlabel('Tonnes Per Day (TPD)', fontsize=12)
                plt.ylabel('Number of Mines', fontsize=12)
                histogram_path = os.path.join(reports_dir, 'mining_rate_histogram.png')
                plt.savefig(histogram_path)
                plt.close() # Close the figure to free memory
                print(f"\n- Distribution histogram saved to: {os.path.basename(histogram_path)}", file=f)

                # --- Box Plot ---
                plt.figure(figsize=(12, 7))
                sns.boxplot(x=plot_data)
                plt.xscale('log')
                plt.title('Box Plot of Calculated Mining Rate (Log Scale)', fontsize=16)
                plt.xlabel('Tonnes Per Day (TPD)', fontsize=12)
                boxplot_path = os.path.join(reports_dir, 'mining_rate_boxplot.png')
                plt.savefig(boxplot_path)
                plt.close() # Close the figure to free memory
                print(f"- Distribution box plot saved to: {os.path.basename(boxplot_path)}", file=f)
                
            else:
                print("\n- Could not generate plots for Mining Rate: No data available.", file=f)
            # ===================================================================






    else:
        print("Filtered Open Pit DataFrame is empty. No report generated.")

else:
    print("Master DataFrame is empty. No analysis to perform.")

conn.close()
print("\nDatabase connection closed. Script finished.")