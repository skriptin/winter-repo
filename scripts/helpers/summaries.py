import pandas as pd

##For Numerical d types (float, int) -> s.t.d mean metc
def generate_numerical_summary(df: pd.DataFrame, column_name: str) -> str:
    if column_name not in df.columns:
        return f"--- Error: Column '{column_name}' not found in DataFrame. ---\n\n"
    summary_stats = df[column_name].describe()
    header = f"--- Numerical Summary for: '{column_name}' ---\n"
    body = summary_stats.to_string()
    
    return f"{header}{body}\n\n"


##For qualitative data types
def generate_qualitative_summary(df: pd.DataFrame, column_name: str) -> str:
    if column_name not in df.columns:
        return f"--- Error: Column '{column_name}' not found in DataFrame. ---\n\n"
        
    value_counts = df[column_name].value_counts(dropna=False) 
    unique_count = df[column_name].nunique()

    # Format the output
    header = f"--- Qualitative Summary for: '{column_name}' ---\n"
    unique_info = f"Total Unique Values: {unique_count}\n\nValue Counts:\n"
    body = value_counts.to_string()
    
    return f"{header}{unique_info}{body}\n\n"
