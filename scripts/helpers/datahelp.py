import pandas as pd
import json  

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
            except json.JSONDecodeError:
                return val
        return val

    for col in wide_df.columns:
        if col != 'report_id':
            # First, cast JSON strings back to Python objects (e.g., lists)
            series = wide_df[col].map(cast_value)
            
            wide_df[col] = pd.to_numeric(series, errors='ignore')

    return wide_df


def save_to_csv(df, pth="dataframe.csv"):
    df.to_csv(pth, index=False )