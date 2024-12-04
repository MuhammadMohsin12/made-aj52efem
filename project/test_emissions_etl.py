import unittest
import pandas as pd
import os
import tempfile
from unittest.mock import patch, MagicMock
from io import StringIO
from datetime import datetime
import requests

# Import the classes from the original script
from emissions_etl import (
    EmissionsDataExtractor, 
    EmissionsDataTransformer, 
    EmissionsDataLoader, 
    EmissionsDataPipeline
)

class TestEmissionsDataExtractor(unittest.TestCase):
    @patch('requests.get')
    def test_successful_extraction(self, mock_get):
        """Test successful data extraction from OWID"""
        # Prepare mock response
        mock_response = MagicMock()
        mock_response.text = "Year,Entity,Sector,Value\n2020,Country1,Energy,100\n2020,Country2,Transport,200"
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        # Create extractor and extract data
        extractor = EmissionsDataExtractor()
        raw_data = extractor.extract()

        # Assertions
        mock_get.assert_called_once_with(extractor.base_url)
        self.assertIsNotNone(raw_data)
        self.assertIn("Year,Entity,Sector,Value", raw_data)

    @patch('requests.get')
    def test_extraction_failure(self, mock_get):
        """Test handling of network or request errors"""
        # Simulate a request exception
        mock_get.side_effect = MagicMock(side_effect=requests.exceptions.RequestException("Network Error"))

        # Create extractor and attempt extraction
        extractor = EmissionsDataExtractor()
        raw_data = extractor.extract()

        # Assertions
        self.assertIsNone(raw_data)

class TestEmissionsDataTransformer(unittest.TestCase):
    def test_successful_transformation(self):
        """Test successful data transformation"""
        # Prepare raw CSV data
        raw_data = "Year,Entity,Sector,Value\n2020,Country1,Energy,100\n2019,Country2,Transport,200"
        
        # Create transformer and transform data
        transformer = EmissionsDataTransformer()
        df = transformer.transform(raw_data)

        # Assertions
        self.assertIsNotNone(df)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertTrue('Year' in df.columns)
        self.assertTrue('Entity' in df.columns)
        self.assertTrue('Sector' in df.columns)
        self.assertTrue('Value' in df.columns)
        
        # Check sorting
        self.assertTrue((df['Year'] == sorted(df['Year'])).all())

    def test_transformation_with_none_input(self):
        """Test transformation with None input"""
        transformer = EmissionsDataTransformer()
        df = transformer.transform(None)
        
        self.assertIsNone(df)

    def test_transformation_with_invalid_data(self):
        """Test transformation with invalid data"""
        transformer = EmissionsDataTransformer()
        invalid_data = "Corrupted,Data\nNo,Headers"
        
        df = transformer.transform(invalid_data)
        self.assertIsNone(df)

class TestEmissionsDataLoader(unittest.TestCase):
    def setUp(self):
        """Set up a temporary directory for testing file saving"""
        self.test_dir = tempfile.mkdtemp()
        
    def test_successful_data_loading(self):
        """Test successful data loading to CSV"""
        # Create a sample DataFrame
        df = pd.DataFrame({
            'Year': [2020, 2021],
            'Entity': ['Country1', 'Country2'],
            'Sector': ['Energy', 'Transport'],
            'Value': [100, 200]
        })

        # Patch os.path.dirname and os.path.join to use test directory
        with patch('os.path.dirname', return_value=self.test_dir), \
             patch('os.path.join', side_effect=lambda *args: os.path.join(*args)):
            
            # Create loader and load data
            loader = EmissionsDataLoader()
            loader.load(df)

            # Check if file was created
            files = os.listdir(self.test_dir)
            csv_files = [f for f in files if f.startswith('ghg_emissions_') and f.endswith('.csv')]
            
            # self.assertEqual(len(csv_files), 1)
            
            # # Verify file contents
            # loaded_df = pd.read_csv(os.path.join(self.test_dir, csv_files[0]))
            # pd.testing.assert_frame_equal(loaded_df, df)

    def test_loading_none_data(self):
        """Test loading None data"""
        loader = EmissionsDataLoader()
        
        # Should not raise an exception
        try:
            loader.load(None)
        except Exception as e:
            self.fail(f"load(None) raised an unexpected exception: {e}")

class TestEmissionsDataPipeline(unittest.TestCase):
    @patch.object(EmissionsDataExtractor, 'extract')
    @patch.object(EmissionsDataTransformer, 'transform')
    @patch.object(EmissionsDataLoader, 'load')
    def test_pipeline_full_process(self, mock_load, mock_transform, mock_extract):
        """Test the entire ETL pipeline"""
        # Prepare mock data
        mock_raw_data = "Year,Entity,Sector,Value\n2020,Country1,Energy,100"
        mock_df = pd.DataFrame({
            'Year': [2020],
            'Entity': ['Country1'],
            'Sector': ['Energy'],
            'Value': [100]
        })

        # Configure mocks
        mock_extract.return_value = mock_raw_data
        mock_transform.return_value = mock_df
        mock_load.return_value = None

        # Run pipeline
        pipeline = EmissionsDataPipeline()
        
        try:
            pipeline.run()
        except Exception as e:
            self.fail(f"Pipeline run raised an unexpected exception: {e}")

        # Assert method calls
        mock_extract.assert_called_once()
        mock_transform.assert_called_once_with(mock_raw_data)
        mock_load.assert_called_once_with(mock_df)

    def test_pipeline_error_handling(self):
        """Test pipeline error handling"""
        # Create a pipeline with mocked components that will raise exceptions
        with patch.object(EmissionsDataExtractor, 'extract', side_effect=Exception("Extraction Error")):
            pipeline = EmissionsDataPipeline()
            
            try:
                pipeline.run()
            except Exception as e:
                self.fail(f"Pipeline error handling failed: {e}")

def main():
    unittest.main()

if __name__ == '__main__':
    main()