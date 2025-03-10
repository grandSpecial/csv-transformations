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
    filters: Optional[str] = Query(None, description="Filters in format 'key1:value1,key2:value2'")
):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    # Initialize filter_dict
    filter_dict = {}
    
    # Parse filters - moved outside of try block
    if filters:
        # Basic format validation
        if ':' not in filters:
            raise HTTPException(
                status_code=400,
                detail="Filters must be in format 'key1:value1,key2:value2'"
            )
        
        try:
            # Convert string "key1:value1,key2:value2" to dictionary
            for item in filters.split(","):
                if ':' not in item:
                    raise HTTPException(
                        status_code=400,
                        detail="Each filter must contain a key and value separated by ':'"
                    )
                key, value = item.split(":", 1)  # Split on first occurrence only
                # Clean up any single or double quotes
                key = key.strip().strip("'\"")
                value = value.strip().strip("'\"")
                if not key or not value:
                    raise HTTPException(
                        status_code=400,
                        detail="Filter keys and values cannot be empty"
                    )
                filter_dict[key] = value
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid filter format. Must be 'key1:value1,key2:value2'. Error: {str(e)}"
            )
    
    try:
        # Create a temporary file to store the uploaded content
        temp_file = "temp_upload.csv"
        with open(temp_file, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Process the file with filters
        result_df = transform_data(temp_file, **filter_dict)
        
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