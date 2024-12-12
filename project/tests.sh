#!/bin/bash


cd "$(dirname "$0")"

echo "Step 1: Installing dependencies..."
pip install -r ../requirements.txt || { echo "Dependency installation failed"; exit 1; }

echo "Step 2: Running test cases for the pipeline..."
echo "Running test: test_bea_etl.py"
python -m unittest ./test_bea_etl.py || { echo "test_bea_etl.py failed"; exit 1; }

echo "Running test: test_emissions_etl.py"
python -m unittest ./test_emissions_etl.py || { echo "test_emissions_etl.py failed"; exit 1; }

echo "Running test: test_file_creation.py"
python -m unittest ./test_file_creation.py || { echo "test_file_creation.py failed"; exit 1; }

echo "All tests completed successfully"
