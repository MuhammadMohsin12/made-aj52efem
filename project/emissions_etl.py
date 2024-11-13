# emissions_etl.py
import pandas as pd
import requests
from io import StringIO
from datetime import datetime
from typing import Optional
import os

class EmissionsDataExtractor:
    """Responsible for extracting data from OWID"""
    
    def __init__(self):
        self.base_url = "https://ourworldindata.org/grapher/ghg-emissions-by-sector.csv"
    
    def extract(self) -> Optional[str]:
        """Extracts raw CSV data from OWID"""
        try:
            response = requests.get(self.base_url)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None

class EmissionsDataTransformer:
    """Responsible for transforming raw emissions data"""
    
    def transform(self, raw_data: Optional[str]) -> Optional[pd.DataFrame]:
        """Transforms raw CSV data into a structured DataFrame"""
        if raw_data is None:
            return None
            
        try:
            # Read the CSV data into a DataFrame
            df = pd.read_csv(StringIO(raw_data))
            
            # Basic data cleaning
            df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
            
            # Sort by year and entity
            df = df.sort_values(['Year', 'Entity'])
            
            return df
            
        except Exception as e:
            print(f"Error transforming data: {e}")
            return None

class EmissionsDataLoader:
    """Responsible for loading transformed emissions data"""
    
    def load(self, data: Optional[pd.DataFrame]) -> None:
        """Loads the transformed data into a CSV file in the data directory"""
        if data is None:
            print("No data to save")
            return
            
        try:
            # Create data directory if it doesn't exist
            data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
            os.makedirs(data_dir, exist_ok=True)
            
            # Generate filename with timestamp
            filename = os.path.join(data_dir, f"ghg_emissions_{datetime.now().strftime('%Y%m%d')}.csv")
            
            # Save to CSV
            data.to_csv(filename, index=False)
            print(f"\nData saved to {filename}")
            
            # Display basic information about the dataset
            print("\nDataset Info:")
            print(data.info())
            print("\nFirst few rows:")
            print(data.head())
            
        except Exception as e:
            print(f"Error saving data: {e}")

class EmissionsDataPipeline:
    """Orchestrates the ETL process for emissions data"""
    
    def __init__(self):
        self.extractor = EmissionsDataExtractor()
        self.transformer = EmissionsDataTransformer()
        self.loader = EmissionsDataLoader()
    
    def run(self):
        """Executes the complete ETL pipeline"""
        try:
            print("Extracting emissions data...")
            raw_data = self.extractor.extract()
            
            print("Transforming emissions data...")
            transformed_data = self.transformer.transform(raw_data)
            
            print("Loading emissions data...")
            self.loader.load(transformed_data)
            
        except Exception as e:
            print(f"Pipeline error: {str(e)}")

def main():
    pipeline = EmissionsDataPipeline()
    pipeline.run()

if __name__ == "__main__":
    main()