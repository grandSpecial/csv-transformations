import pandas as pd

def transform_data(filename, **filters):
    """
    Get only numeric fields with question text in column 1: 
    Columns 1 - 11 should be the answer option, and the values should be the count.
    Create three columns for answer buckets: low, moderate, high.
    Create an average column.
    Allows filtering before melting and pivoting based on any categorical column.
    
    Args:
        filename (str): Path to the CSV file.
        **filters: Key-value pairs to filter the dataset before processing.
    
    Returns:
        pd.DataFrame: Transformed dataset.
    """
    # Load the dataset
    df = pd.read_csv(filename)
    
    # Drop unwanted columns
    if "Network ID" in df.columns:
        df.drop(columns=["Network ID"], inplace=True)

    # Apply filtering based on provided arguments
    for col, value in filters.items():
        if col in df.columns:
            df = df[df[col] == value]

    # Melt the dataframe
    df_melt = df.melt(id_vars=["#"], var_name="Question", value_name="Answer")

    # Convert Answer to numeric and drop invalid rows
    df_melt['Answer'] = pd.to_numeric(df_melt['Answer'], errors='coerce')
    df_melt.dropna(inplace=True)
    df_melt["Answer"] = df_melt["Answer"].astype(int)

    # Pivot the table
    df_pivot = df_melt.pivot_table(index='Question', columns='Answer', aggfunc='size', fill_value=0)

    # Ensure only valid answer columns (0-10)
    valid_columns = list(range(0, 11))
    df_pivot = df_pivot.reindex(columns=valid_columns, fill_value=0)

    # Reset index
    df_pivot.reset_index(inplace=True)

    # Compute categorical percentages
    total_counts = df_pivot.loc[:, range(0, 11)].sum(axis=1)
    df_pivot["Low"] = round(df_pivot.loc[:, range(0, 7)].sum(axis=1) / total_counts, 2)
    df_pivot["Mod"] = round(df_pivot.loc[:, range(7, 9)].sum(axis=1) / total_counts, 2)
    df_pivot["High"] = round(df_pivot.loc[:, range(9, 11)].sum(axis=1) / total_counts, 2)

    # Compute average response score
    df_pivot["Avg"] = round((df_pivot.loc[:, range(0, 11)] * pd.Series(range(0, 11), index=range(0, 11))).sum(axis=1) / total_counts, 2)

    return df_pivot 