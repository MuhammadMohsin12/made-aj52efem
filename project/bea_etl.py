import requests
import pandas as pd
from typing import Dict, Any, Tuple
from datetime import datetime
from abc import ABC, abstractmethod
import os 

class DataExtractor:
    """Responsible for extracting data from the BEA API"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://apps.bea.gov/api/data"
    
    def extract(self) -> Dict[str, Any]:
        params = {
            "UserID": self.api_key,
            "method": "GetData",
            "datasetname": "GDPbyIndustry",
            "TableID": "1",
            "Frequency": "A",  # Annual data
            "Year": "2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020",
            "Industry": "ALL",
            "ResultFormat": "json"
        }
        
        response = requests.get(self.base_url, params=params)
        
        if response.status_code != 200:
            raise Exception(f"API request failed with status code: {response.status_code}")
            
        return response.json()

class DataTransformer:
    """Responsible for transforming raw data into the desired format"""
    
    def transform(self, raw_data: Dict[str, Any]) -> Tuple[pd.DataFrame, str, str]:
        """Transforms raw JSON data into a structured DataFrame"""
        try:
            # Extracting data
            results = raw_data["BEAAPI"]["Results"]
            if isinstance(results, list):
                series = results[0]["Data"]
            else:
                series = results["Data"]
            
            # Creating DataFrame from the series
            df = pd.DataFrame(series)
            
            # Normalize column names
            if 'IndustrYDescription' in df.columns:
                df = df.rename(columns={'IndustrYDescription': 'IndustryDescription'})
            
            # Convert "DataValue" column to numeric
            df["DataValue"] = pd.to_numeric(df["DataValue"], errors="coerce")
            
            # Pivot the data for a cleaner format
            pivoted_df = pd.pivot_table(
                df,
                values='DataValue',
                index=['Industry', 'IndustryDescription'],
                columns=['Year'],
                aggfunc='first'
            ).reset_index()
            
            # Generate line numbers and add descriptions
            pivoted_df['Line'] = range(1, len(pivoted_df) + 1)
            pivoted_df['Description'] = pivoted_df['IndustryDescription']
            
            # Sorting by line numbers
            pivoted_df = pivoted_df.sort_values('Line')
            
            # Define the final columns to include
            year_cols = [str(year) for year in range(2010, 2021)]
            final_cols = ['Line', 'Description'] + year_cols
            
            final_df = pivoted_df[final_cols]
            
            # Format numeric columns to one decimal place
            for year in year_cols:
                if year in final_df.columns:
                    final_df[year] = final_df[year].round(1)
            
            # Title and last revised information
            title = "Value Added by Industry"
            last_revised = f"Last Revised on: {datetime.now().strftime('%B %d, %Y')}"
            
            return final_df, title, last_revised
            
        except Exception as e:
            print(f"\nDetailed error information:")
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            import traceback
            print(traceback.format_exc())
            raise

class DataLoader:
    """Responsible for loading transformed data into the data directory"""
    
    def load(self, data: Tuple[pd.DataFrame, str, str]) -> None:
        """Loads the transformed data into a CSV file"""
        df, title, last_revised = data
        
        try:
            # Create data directory if it doesn't exist
            data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
            os.makedirs(data_dir, exist_ok=True)
            
            # Generate filename
            output_file = os.path.join(data_dir, f"gdp_by_industry_{datetime.now().strftime('%Y%m%d')}.csv")
            
            # Save to CSV
            df.to_csv(output_file, index=False)
            
            # Display information
            print(f"\n{title}")
            print(last_revised)
            print("\nProcessed data:")
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            pd.set_option('display.max_rows', None)
            print(df)
            print(f"\nData saved to {output_file}")
            
        except Exception as e:
            print(f"Error saving data: {e}")

class BEADataPipeline:
    """Orchestrates the ETL process"""
    
    def __init__(self, api_key: str):
        self.extractor = DataExtractor(api_key)
        self.transformer = DataTransformer()
        self.loader = DataLoader()
    
    def run(self):
        """Executes the complete ETL pipeline"""
        try:
            print("Extracting data...")
            raw_data = self.extractor.extract()
            
            print("Transforming data...")
            transformed_data = self.transformer.transform(raw_data)
            
            print("Loading data...")
            self.loader.load(transformed_data)
            
        except Exception as e:
            print(f"Pipeline error: {str(e)}")

def main():
    api_key = '2B9CC899-D595-4CE0-9E7D-E555717EF787'
    pipeline = BEADataPipeline(api_key)
    pipeline.run()

if __name__ == "__main__":
    main()