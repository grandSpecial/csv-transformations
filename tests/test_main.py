import pytest
from fastapi.testclient import TestClient
import pandas as pd
import os
import sys

from app.main import app

# Use simple initialization
client = TestClient(app)

@pytest.fixture
def sample_csv():
    # Create a sample CSV file for testing
    df = pd.DataFrame({
        '#': ["asd23","ijsd9f8u","09sudf","0sjdf0","09suifd"],
        'Question 1': ['A', 'B', 'A', 'B', 'C'],
        'Question 2': [1, 2, 3, 4, 5]
    })
    filename = "test_sample.csv"
    df.to_csv(filename, index=False)
    yield filename
    # Cleanup after tests
    if os.path.exists(filename):
        os.remove(filename)

def test_create_counts_table_no_filters(sample_csv):
    with open(sample_csv, "rb") as f:
        response = client.post(
            "/create_counts_table",
            files={"file": ("test.csv", f, "text/csv")}
        )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    # Add more specific assertions based on your transform_data function

def test_create_counts_table_with_filters(sample_csv):
    with open(sample_csv, "rb") as f:
        response = client.post(
            "/create_counts_table",
            files={"file": ("test.csv", f, "text/csv")},
            params={"filters": "category:A"}
        )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    # Add more specific assertions based on your transform_data function

def test_invalid_file_extension():
    # Test with a non-CSV file
    response = client.post(
        "/create_counts_table",
        files={"file": ("test.txt", b"some content", "text/plain")}
    )
    assert response.status_code == 400
    assert response.json()["detail"] == "File must be a CSV"

def test_invalid_filter_format():
    # Create a sample CSV with proper structure
    df = pd.DataFrame({
        '#': ["test1"],
        'Question 1': ['A'],
        'Question 2': [1]
    })
    filename = "test_invalid_filter.csv"
    df.to_csv(filename, index=False)
    
    try:
        # Test case 1: Missing colon
        with open(filename, "rb") as f:
            response = client.post(
                "/create_counts_table",
                files={"file": ("test.csv", f, "text/csv")},
                params={"filters": "invalid_format"}
            )
            assert response.status_code == 400
            assert "Filters must be in format" in response.json()["detail"]

        # Test case 2: Empty value
        with open(filename, "rb") as f:
            response = client.post(
                "/create_counts_table",
                files={"file": ("test.csv", f, "text/csv")},
                params={"filters": "key:"}
            )
            assert response.status_code == 400
            assert "Filter keys and values cannot be empty" in response.json()["detail"]
        
    finally:
        # Clean up the test file
        if os.path.exists(filename):
            os.remove(filename) 