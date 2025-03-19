from fastapi import FastAPI, UploadFile, HTTPException, Query
from fastapi.responses import JSONResponse
import pandas as pd
import os
from typing import Optional, Dict
from app.utils import transform_data

app = FastAPI()

@app.post("/create_counts_table")
async def create_counts_table(
    file: UploadFile,
    filters: Optional[str] = Query(
        None, description="Filters in format 'key1 operator value, key2 operator value'. Example: 'Age >= 30, Gender = Female, Avg >= 4.5'"
    ),
    group_filter: Optional[str] = Query(
        None, description="Filter by group membership (Low, Mod, High) for a specific question. Example: 'I am excited to work most days.:Low'"
    )
):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    # Initialize filter dictionaries
    pre_transform_filters = {}  # Filters applied before transformation
    post_transform_filters = {}  # Filters applied after transformation
    group_filter_dict = None  # Group-based filter

    # Define valid operators
    valid_operators = {"=", "!=", ">=", "<=", ">", "<"}

    # Parse filters (pre-transform and post-transform)
    if filters:
        try:
            for item in filters.split(","):
                parts = item.strip().split(" ", 2)  # Split into max 3 parts: column, operator, value
                if len(parts) != 3:
                    raise HTTPException(
                        status_code=400,
                        detail="Each filter must be in the format 'column operator value'. Example: 'Age >= 30, Gender = Female, Avg >= 4.5'"
                    )

                col, op, val = parts
                col = col.strip()
                op = op.strip()
                val = val.strip().strip("'\"")  # Remove surrounding quotes if present

                if op not in valid_operators:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Invalid operator '{op}'. Allowed operators: {', '.join(valid_operators)}"
                    )

                if not col or not val:
                    raise HTTPException(
                        status_code=400,
                        detail="Filter keys and values cannot be empty"
                    )

                # Convert numeric values properly
                if val.replace('.', '', 1).isdigit():  # Checks if it's a number (int or float)
                    val = float(val) if '.' in val else int(val)

                # Separate filters for original dataset vs. computed columns
                if col in ["Low", "Mod", "High", "Avg"]:
                    post_transform_filters[f"{col} {op}"] = val
                else:
                    pre_transform_filters[f"{col} {op}"] = val

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid filter format. Must be 'column operator value'. Error: {str(e)}"
            )
    
    # Parse group-based filter (if provided)
    if group_filter:
        try:
            # Decode URL encoding and split only on the LAST colon
            parts = group_filter.rsplit(":", 1)  # This ensures only the last ':' is used for splitting

            if len(parts) != 2:
                raise HTTPException(
                    status_code=400,
                    detail="Group filter must be in the format 'Question:Group'. Example: 'I am excited to work most days.:Low'"
                )

            question, group = parts
            question = question.strip()  # Trim spaces from question
            group = group.strip()  # Trim spaces from group

            if group not in ["Low", "Mod", "High"]:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid group '{group}'. Must be 'Low', 'Mod', or 'High'."
                )

            # Ensure group filter is correctly formatted
            group_filter_dict = {"question": question, "group": group}

        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid group filter format. Must be 'Question:Group'. Error: {str(e)}"
            )

    try:
        # Create a temporary file to store the uploaded content
        temp_file = "temp_upload.csv"
        with open(temp_file, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Process the file with pre-transform filters
        result_df = transform_data(temp_file, group_filter=group_filter_dict, **pre_transform_filters)

        # Apply post-transform filters on "Low", "Mod", "High", "Avg"
        if result_df is not None and not result_df.empty:
            for key, value in post_transform_filters.items():
                col, op = key.split(" ", 1)  # Extract column and operator
                if col in result_df.columns:
                    result_df = result_df[eval(f"result_df[col] {op} value")]

        # Convert the DataFrame to a dictionary
        result = result_df.to_dict(orient='records')
        
        # Clean up the temporary file
        os.remove(temp_file)
        
        return JSONResponse(content=result)
    
    except Exception as e:
        # Clean up the temporary file if it exists
        if os.path.exists("temp_upload.csv"):
            os.remove("temp_upload.csv")
        raise HTTPException(status_code=500, detail=str(e))
