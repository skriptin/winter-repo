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

df["deposit_type"] = df["deposit_type"].apply(exploder.clean_entry)
df["deposit_type"] = df["deposit_type"].apply(exploder.create_combination_key)

df["underground_mining_method"] = df["underground_mining_method"].apply(exploder.clean_entry)
df["underground_mining_method"] = df["underground_mining_method"].apply(exploder.create_combination_key)


cols_to_make_numeric = [

    # 🔹 Mining Costs
    'stripping_ratio',
    'open_pit_mining_cost_dollars_per_t_mined_or_moved',
    'open_pit_mining_cost_dollars_per_t_milled_or_processed',
    "underground_mining_cost_dollars_per_t_mined_or_moved",
    "underground_mining_cost_dollars_per_t_milled_or_processed",
    'g_and_a_cost_dollars_per_t_milled',
    'total_operating_cost_dollars_per_t_milled',
    "processing_cost_dollars_per_t_milled",

    # 🔹 Production / Scale
    'initial_capex_in_millions',
    'life_of_mine',
    'processing_rate',
    'total_ore_mined',
    'total_waste_mined',
    'total_material_mined',

    # Metals
    'copper_price',
    'copper_cut_off_grade',
    'copper_metallurgical_recovery',

    'gold_price',
    'gold_cut_off_grade',
    'gold_metallurgical_recovery',

    'silver_price',
    'silver_cut_off_grade',
    'silver_metallurgical_recovery',

    'zinc_price',
    "zinc_cut_off_grade",
    'zinc_metallurgical_recovery',

    'lead_price',
    "lead_cutoff_grade"
    'lead_metallurgical_recovery',

    'iron_price',
    "iron_cutoff_grade",
    'iron_metallurgical_recovery',

    # 🔹 Exchange Rates
    'aud_usd_exchange_rate',
    'cad_usd_exchange_rate',

    # 🔹 Financial Metrics
    'pre_tax_npv_5_in_millions',
    'pre_tax_npv_8_in_millions',
    'pre_tax_npv_10_in_millions',
    "after_tax_npv_5_in_millions",
    "after_tax_npv_8_in_millions",
    "after_tax_npv_10_in_millions",
    'pre_tax_irr',
    'after_tax_irr',
]
excluded_cols = [
    "report_id", "subdivision", "lat", "long", "author_company","company_name", "effective_date",
    "project_name"
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
for col in [c for c in df.columns if c not in cols_to_make_numeric and c not in excluded_cols]:
    try:
        print(f"Analyzing: {col}...")
        report_parts.append(summaries.generate_qualitative_summary(df, col))
    except Exception as e:
        print(f"  > AN EXCEPTION OCCURRED while processing column: '{col}'. Error: {e}")

for col in excluded_cols:
    try:
        print(f"Analyzing: {col}...")
        report_parts.append(summaries.generate_null_summary(df, col))
    except Exception as e:
        print(f"  > AN EXCEPTION OCCURRED while processing column: '{col}'. Error: {e}")

full_report = "".join(report_parts)


with open("../reports/overall_summary.txt", "w", encoding="utf-8") as f:
    f.write("      DataFrame Overall Summary Report     \n")
    f.write("../reports/overall_summary.txt")
    f.write(full_report)