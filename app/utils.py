import pandas as pd
import operator

def transform_data(filename, group_filter=None, **filters):
    """
    Get only numeric fields with question text in column 1:
    Columns 1 - 11 should be the answer option, and the values should be the count.
    Create three columns for answer buckets: low, moderate, high.
    Create an average column.
    Allows filtering before melting and pivoting based on any categorical column.
    Supports flexible filtering with operators: =, >=, <=, !=
    Also supports filtering based on Low, Mod, High category membership for a specific question.

    Args:
        filename (str): Path to the CSV file.
        group_filter (dict, optional): Filters based on category membership.
                                       Example: {"question": "I am excited to work most days.", "group": "Low"}
        **filters: Key-value pairs to filter the dataset before processing.
    
    Returns:
        pd.DataFrame: Always returns df_pivot, even when filtering by group membership.
    """
    # Load the dataset
    df = pd.read_csv(filename)
    
    # Drop unwanted columns
    if "Network ID" in df.columns:
        df.drop(columns=["Network ID"], inplace=True)

    # Trim column names to avoid trailing spaces
    df.columns = df.columns.str.strip()

    # Define valid operators
    ops = {
        "=": operator.eq,
        "!=": operator.ne,
        ">=": operator.ge,
        "<=": operator.le,
        ">": operator.gt,
        "<": operator.lt
    }

    # Apply filtering based on provided arguments
    for key, value in filters.items():
        parts = key.split(" ")
        if len(parts) == 2:
            col, op = parts
            if col in df.columns and op in ops:
                df = df[ops[op](df[col], value)]
        else:
            raise ValueError(f"Invalid filter format: {key}. Expected 'column operator value'.")

    # 🔹 **Find Respondents in Selected Group Before Pivoting**
    if group_filter:
        question = group_filter.get("question")
        group = group_filter.get("group")

        # Validate question
        if question not in df.columns:
            raise ValueError(f"Question '{question}' not found in original dataset.")

        # Validate group
        if group not in ["Low", "Mod", "High"]:
            raise ValueError("Invalid group. Must be 'Low', 'Mod', or 'High'.")

        # Melt dataset to long format
        df_melt = df.melt(id_vars=["#"], var_name="Question", value_name="Answer")
        df_melt['Answer'] = pd.to_numeric(df_melt['Answer'], errors='coerce')
        df_melt.dropna(inplace=True)
        df_melt["Answer"] = df_melt["Answer"].astype(int)

        # Identify respondents who belong to the selected group
        if group == "Low":
            respondent_ids = df_melt[
                (df_melt["Question"] == question) & (df_melt["Answer"].between(0, 6))
            ]["#"].unique()
        elif group == "Mod":
            respondent_ids = df_melt[
                (df_melt["Question"] == question) & (df_melt["Answer"].between(7, 8))
            ]["#"].unique()
        elif group == "High":
            respondent_ids = df_melt[
                (df_melt["Question"] == question) & (df_melt["Answer"].between(9, 10))
            ]["#"].unique()

        # Filter dataset based on selected respondents
        df = df[df["#"].isin(respondent_ids)]

    # 🔹 **Proceed with Normal Transformation After Filtering**
    df_melt = df.melt(id_vars=["#"], var_name="Question", value_name="Answer")
    df_melt['Answer'] = pd.to_numeric(df_melt['Answer'], errors='coerce')
    df_melt.dropna(inplace=True)
    df_melt["Answer"] = df_melt["Answer"].astype(int)

    df_pivot = df_melt.pivot_table(index='Question', columns='Answer', aggfunc='size', fill_value=0)
    df_pivot = df_pivot.reindex(columns=list(range(0, 11)), fill_value=0)
    df_pivot.reset_index(inplace=True)

    # Compute final categorical percentages
    total_counts = df_pivot.loc[:, range(0, 11)].sum(axis=1).replace(0, 1)
    df_pivot["Low"] = round(df_pivot.loc[:, range(0, 7)].sum(axis=1) / total_counts, 2)
    df_pivot["Mod"] = round(df_pivot.loc[:, range(7, 9)].sum(axis=1) / total_counts, 2)
    df_pivot["High"] = round(df_pivot.loc[:, range(9, 11)].sum(axis=1) / total_counts, 2)
    df_pivot["Avg"] = round(
        (df_pivot.loc[:, range(0, 11)] * pd.Series(range(0, 11), index=range(0, 11))).sum(axis=1) / total_counts, 2
    )

    # Ensure all computed columns exist and have float type
    for col in ["Low", "Mod", "High", "Avg"]:
        df_pivot[col] = df_pivot[col].astype(float).fillna(0.0)

    return df_pivot
