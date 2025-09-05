import pandas as pd

def generate_numerical_summary(df: pd.DataFrame, column_name: str) -> str:
    if column_name not in df.columns:
        return f"--- Error: Column '{column_name}' not found in DataFrame. ---\n\n"

    summary_stats = df[column_name].describe()
    total_samples = len(df)
    non_null_count = int(summary_stats["count"])
    null_count = total_samples - non_null_count

    header = f"--- Numerical Summary for: '{column_name}' ---\n"
    sample_info = (
        f"Total Samples : {total_samples}\n"
        f"Null Count    : {null_count}\n\n"
    )
    body = summary_stats.to_string()
    
    return f"{header}{sample_info}{body}\n\n"


## For Qualitative data types (categorical, object)
def generate_qualitative_summary(df: pd.DataFrame, column_name: str) -> str:
    if column_name not in df.columns:
        return f"--- Error: Column '{column_name}' not found in DataFrame. ---\n\n"
        
    value_counts = df[column_name].value_counts(dropna=False)
    unique_count = df[column_name].nunique(dropna=True)
    total_samples = len(df)
    non_null_count = df[column_name].notna().sum()
    null_count = total_samples - non_null_count

    header = f"--- Qualitative Summary for: '{column_name}' ---\n"
    sample_info = (
        f"Total Samples : {total_samples}\n"
        f"Null Count    : {null_count}\n"
        f"Unique Values : {unique_count}\n\n"
    )
    body = value_counts.to_string()

    return f"{header}{sample_info}Value Counts:\n{body}\n\n"



def generate_null_summary(df: pd.DataFrame, column_name: str) -> str:
    if column_name not in df.columns:
        return f"--- Error: Column '{column_name}' not found in DataFrame. ---\n\n"

    total_samples = len(df)
    non_null_count = df[column_name].notna().sum()
    null_count = total_samples - non_null_count

    header = f"--- Null Summary for: {column_name} ---\n"
    sample_info = (
        f"Total Samples : {total_samples}\n"
        f"Null Count    : {null_count}\n\n"
    )

    return f"{header}{sample_info}"
