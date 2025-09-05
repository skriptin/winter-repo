from helpers import exploder, datahelp, summaries
import pandas as pd
import os

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000) 
pd.set_option('display.max_colwidth', None) 

df = pd.read_csv("dataframe.csv")


# Theese columns are lists that need to be cleaned
df["processing_method"] = df["processing_method"].apply(exploder.clean_entry)
df["processing_method"] = df["processing_method"].apply(exploder.create_combination_key)

# df["deposite_type"] = df["deposite_type"].apply(exploder.clean_entry)
# df["deposite_type"] = df["deposit_type"].apply(exploder.create_combination_key)

(df["report_id"].astype(str) + df["processing_method"].astype(str)) \
    .to_csv("../reports/processing/processing_method.txt", index=False, header=False)

cols_to_make_numeric = [
    'stripping_ratio', 'open_pit_mining_cost_dollars_per_t_mined_or_moved',
    'total_operating_cost_dollars_per_t_milled', 'initial_capex_in_millions',
    'life_of_mine', 'processing_rate', 'total_ore_mined', 'total_waste_mined',
    'copper_price', 'gold_price', 'silver_price', 'copper_cut_off_grade',
    'gold_cut_off_grade', 'copper_metallurgical_recovery', 'gold_metallurgical_recovery',
    'pre_tax_npv_8_in_millions', 'after_tax_irr'
]
report_parts = []

for col in cols_to_make_numeric:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

print("--- Generating Numerical Summaries ---")
for col in cols_to_make_numeric:
    print(f"Analyzing: {col}...")
    report_parts.append(summaries.generate_numerical_summary(df, col))

print("\n--- Generating Qualitative Summaries ---")
for col in [c for c in df.columns if c not in cols_to_make_numeric]:
    try:
        print(f"Analyzing: {col}...")
        report_parts.append(summaries.generate_qualitative_summary(df, col))
    except Exception as e:
        print(f"  > AN EXCEPTION OCCURRED while processing column: '{col}'. Error: {e}")


full_report = "".join(report_parts)


with open("../reports/overall_summary.txt", "w", encoding="utf-8") as f:
    f.write("      DataFrame Overall Summary Report     \n")
    f.write("../reports/overall_summary.txt")
    f.write(full_report)