from fastapi import FastAPI, UploadFile, HTTPException, Query, Depends
from fastapi.responses import JSONResponse
import json
import pandas as pd
import os
from typing import Optional, Dict
from app.utils import PreProcess, summarize, build_csv_from_typeform, get_typeforms
from app.security import verify_api_key
from urllib.parse import unquote

app = FastAPI()

@app.post("/create_counts_table")
async def create_counts_table(
    file: UploadFile,
    filters: Optional[str] = Query(
        None, description="Filters in format 'key1 operator value; key2 operator value'. Example: 'Age >= 30; Gender = Female; Avg >= 4.5'. Use semicolons to separate multiple filters."
    ),
    group_filter: Optional[str] = Query(
        None, description="Filter by group membership (Low, Mod, High) for a specific question. Example: 'I am excited to work most days.:Low'"
    ),
    _: bool = Depends(verify_api_key),  # Move auth to the end
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
            # URL decode the filters string
            filters = unquote(filters)
            print("Decoded filters:", filters)
            
            # Split on semicolons instead of commas
            for item in filters.split(";"):
                item = item.strip()
                if not item:  # Skip empty filters
                    continue
                    
                print(f"\nProcessing filter item: '{item}'")
                
                # Find the last occurrence of each operator to handle spaces in column names
                last_operator_pos = -1
                last_operator = None
                
                # First, try to find the last occurrence of each operator
                print("Trying to find operators with spaces...")
                for op in sorted(valid_operators, key=len, reverse=True):  # Sort by length to match longer operators first
                    # Look for the operator with spaces around it to avoid matching parts of words
                    search_str = f" {op} "
                    pos = item.rfind(search_str)
                    print(f"Looking for '{search_str}' in '{item}', found at position: {pos}")
                    if pos > last_operator_pos:
                        last_operator_pos = pos
                        last_operator = op
                        print(f"Found operator '{op}' at position {pos}")
                
                # If no operator found with spaces, try without spaces
                if last_operator_pos == -1:
                    print("No operators found with spaces, trying without spaces...")
                    for op in sorted(valid_operators, key=len, reverse=True):
                        pos = item.rfind(op)
                        print(f"Looking for '{op}' in '{item}', found at position: {pos}")
                        if pos > last_operator_pos:
                            last_operator_pos = pos
                            last_operator = op
                            print(f"Found operator '{op}' at position {pos}")

                if last_operator_pos == -1:
                    print("No valid operator found in the filter")
                    raise HTTPException(
                        status_code=400,
                        detail=f"No valid operator found in filter: {item}"
                    )

                # Split on the last occurrence of the operator
                col = item[:last_operator_pos].strip()
                # Get the value part and remove any leading operator
                val = item[last_operator_pos + len(last_operator):].strip().strip("'\"")  # Remove surrounding quotes if present
                # Remove any leading operator from the value
                for op in valid_operators:
                    if val.startswith(op):
                        val = val[len(op):].strip()
                        break

                if not col or not val:
                    print("Empty column or value after splitting")
                    raise HTTPException(
                        status_code=400,
                        detail="Filter keys and values cannot be empty"
                    )

                print(f"Successfully parsed filter - Column: '{col}', Operator: '{last_operator}', Value: '{val}'")

                # Convert numeric values properly
                if val.replace('.', '', 1).isdigit():  # Checks if it's a number (int or float)
                    val = float(val) if '.' in val else int(val)

                # Separate filters for original dataset vs. computed columns
                if col in ["Low", "Mod", "High", "Avg"]:
                    post_transform_filters[col] = {"operator": last_operator, "value": val}
                else:
                    pre_transform_filters[col] = {"operator": last_operator, "value": val}

        except HTTPException:
            raise
        except Exception as e:
            print(f"Error processing filter: {str(e)}")
            print(f"Error type: {type(e)}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
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
        print("temp file")
        temp_file = "temp_upload.csv"
        with open(temp_file, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Process the file with pre-transform filters
        PP = PreProcess(temp_file, group_filter=group_filter_dict, **pre_transform_filters)
        print("data back")
        result_df = PP.count_data() 
        print("file processed")
        # Apply post-transform filters on "Low", "Mod", "High", "Avg"
        if result_df is not None and not result_df.empty:
            for col, filter_info in post_transform_filters.items():
                op = filter_info["operator"]
                value = filter_info["value"]
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

@app.post("/create_correlation_matrix")
async def create_correlation_matrix(
    file: UploadFile,
    filters: Optional[str] = Query(
        None, 
        description="Filters in format 'key1 operator value, key2 operator value'. Example: 'Age >= 30, Gender = Female, Avg >= 4.5'"
    ),
    group_filter: Optional[str] = Query(
        None, 
        description="Filter by group membership (Low, Mod, High) for a specific question. Example: 'I am excited to work most days.:Low'"
    ),
    _: bool = Depends(verify_api_key),  # Move auth to the end
):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    # Initialize filter dictionaries
    pre_transform_filters = {}  # Filters applied before transformation
    post_transform_filters = {}  # Filters applied after transformation
    group_filter_dict = None      # Group-based filter

    # Define valid operators
    valid_operators = {"=", "!=", ">=", "<=", ">", "<"}

    # Parse filters (pre-transform and post-transform)
    if filters:
        try:
            for item in filters.split(","):
                # Find the last occurrence of each operator to handle spaces in column names
                last_operator_pos = -1
                last_operator = None
                for op in valid_operators:
                    pos = item.rfind(op)
                    if pos > last_operator_pos:
                        last_operator_pos = pos
                        last_operator = op

                if last_operator_pos == -1:
                    raise HTTPException(
                        status_code=400,
                        detail=f"No valid operator found in filter: {item}"
                    )

                # Split on the last occurrence of the operator
                col = item[:last_operator_pos].strip()
                # Get the value part and remove any leading operator
                val = item[last_operator_pos + len(last_operator):].strip().strip("'\"")  # Remove surrounding quotes if present
                # Remove any leading operator from the value
                for op in valid_operators:
                    if val.startswith(op):
                        val = val[len(op):].strip()
                        break

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
                    post_transform_filters[col] = {"operator": last_operator, "value": val}
                else:
                    pre_transform_filters[col] = {"operator": last_operator, "value": val}

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
            parts = group_filter.rsplit(":", 1)  # Split on the last colon
            if len(parts) != 2:
                raise HTTPException(
                    status_code=400,
                    detail="Group filter must be in the format 'Question:Group'. Example: 'I am excited to work most days.:Low'"
                )
            question, group = parts
            question = question.strip()
            group = group.strip()
            if group not in ["Low", "Mod", "High"]:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid group '{group}'. Must be 'Low', 'Mod', or 'High'."
                )
            group_filter_dict = {"question": question, "group": group}
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid group filter format. Must be 'Question:Group'. Error: {str(e)}"
            )

    try:
        # Create a temporary file to store the uploaded content
        print("temp file")
        temp_file = "temp_upload.csv"
        with open(temp_file, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Process the file with pre-transform filters
        PP = PreProcess(temp_file, group_filter=group_filter_dict, **pre_transform_filters)
        result_df = PP.correlate_data() 
        
        # Apply post-transform filters if they apply to the correlation matrix (ensure these filters match column names)
        if result_df is not None and not result_df.empty:
            for col, filter_info in post_transform_filters.items():
                op = filter_info["operator"]
                value = filter_info["value"]
                if col in result_df.columns:
                    result_df = result_df[eval(f"result_df[col] {op} value")]
        
        # Convert the DataFrame to a dictionary for JSON response
        result = result_df.to_dict(orient='records')
        
        # Clean up the temporary file
        os.remove(temp_file)
        
        return JSONResponse(content=result)
    
    except Exception as e:
        if os.path.exists("temp_upload.csv"):
            os.remove("temp_upload.csv")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/summarize")
async def summarize_endpoint(
    file: UploadFile,
    question: str = Query(..., description="The question whose responses you want to summarize"),
    filters: Optional[str] = Query(
        None, 
        description="Optional filters in format 'column operator value, column operator value'"
    ),
    group_filter: Optional[str] = Query(
        None, 
        description="Optional group filter in format 'Question:Group'. Example: 'I am excited to work most days.:Low'"
    ),
    _: bool = Depends(verify_api_key),  # Move auth to the end
):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    # Parse filters string into a dictionary (if provided)
    parsed_filters = {}
    if filters:
        try:
            for item in filters.split(","):
                parts = item.strip().split(" ", 2)  # Expecting 'column operator value'
                if len(parts) != 3:
                    raise HTTPException(
                        status_code=400,
                        detail="Each filter must be in the format 'column operator value'."
                    )
                key, op, val = parts
                key = key.strip()
                op = op.strip()
                val = val.strip().strip("'\"")
                
                # Convert numeric values if applicable
                if val.replace('.', '', 1).isdigit():
                    val = float(val) if '.' in val else int(val)
                
                parsed_filters[f"{key} {op}"] = val
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error parsing filters: {str(e)}")
    
    # Parse group_filter string into a dictionary if provided
    group_filter_dict = None
    if group_filter:
        try:
            parts = group_filter.rsplit(":", 1)  # Split on the last colon
            if len(parts) != 2:
                raise HTTPException(
                    status_code=400,
                    detail="Group filter must be in the format 'Question:Group'."
                )
            question_group, group = parts
            question_group = question_group.strip()
            group = group.strip()
            if group not in ["Low", "Mod", "High"]:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid group specified. Must be 'Low', 'Mod', or 'High'."
                )
            group_filter_dict = {"question": question_group, "group": group}
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error parsing group filter: {str(e)}")
    
    try:
        # Write the uploaded file to a temporary location
        temp_file = "temp_upload.csv"
        with open(temp_file, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Call the summarize function with the temporary file, question, group_filter, and filters
        result = summarize(temp_file, question, group_filter=group_filter_dict, **parsed_filters)
        
        # Clean up the temporary file
        os.remove(temp_file)
        
        # Since the summarize function returns a JSON string, parse it before returning
        return JSONResponse(content=json.loads(result))
    except Exception as e:
        # Clean up the temporary file if it still exists
        if os.path.exists("temp_upload.csv"):
            os.remove("temp_upload.csv")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/get_forms")
def get_forms(_: bool = Depends(verify_api_key)):
    return get_typeforms()

@app.post("/get_csv")
async def get_csv(
    form_id: str,
    _: bool = Depends(verify_api_key)
):
    return build_csv_from_typeform(form_id)
