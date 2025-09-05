import pandas as pd
import os
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
from tqdm import tqdm

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

def plot_numeric_histograms_to_pdf(
    df,
    cols_to_plot,
    save_path,
    log_scale=False,
    outlier_protection=True,
    cap_std=5,
    bins=100,
):
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    with PdfPages(save_path) as pdf:
        for col in tqdm(cols_to_plot, desc="Generating Histograms"):
            if col not in df.columns:
                print(f"Skipping '{col}' (not found in DataFrame).")
                continue

            series = df[col].dropna()
            if series.empty:
                print(f"Skipping '{col}' (all values are null).")
                continue

            plt.figure(figsize=(8, 6))
            plot_data = series
            title = f"Histogram of {col}"

            if outlier_protection:
                median = series.median()
                std = series.std()
                lower_bound = median - cap_std * std
                upper_bound = median + cap_std * std

                plot_data = series[
                    (series >= lower_bound) & (series <= upper_bound)
                ]
                removed_count = len(series) - len(plot_data)
                title = f"Histogram of {col} (data within {cap_std}σ of median)"

                if removed_count > 0:
                    ax = plt.gca()
                    ax.text(
                        0.95,
                        0.95,
                        f"Removed samples: {removed_count}",
                        transform=ax.transAxes,
                        fontsize=10,
                        color="red",
                        verticalalignment="top",
                        horizontalalignment="right",
                    )

            plt.hist(plot_data, bins=bins, color="steelblue", edgecolor="black")
            plt.title(title, fontsize=14)
            plt.xlabel(col, fontsize=12)
            plt.ylabel("Frequency", fontsize=12)

            if log_scale:
                plt.yscale("log")

            plt.grid(True, linestyle="--", alpha=0.6)
            pdf.savefig()
            plt.close()