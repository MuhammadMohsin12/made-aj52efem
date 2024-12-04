#!/bin/bash

# Install dependencies
pip install -r ../requirements.txt

# Run ETL pipeline
python3 run_etl.py

#Run Test cases for the pipeline
python -m unittest .\test_bea_etl.py
python -m unittest .\test_emissions_etl.py
python -m unittest .\test_file_creation.py