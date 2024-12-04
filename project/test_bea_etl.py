import unittest
import pandas as pd
from unittest.mock import patch, MagicMock
from bea_etl import DataExtractor, DataTransformer, DataLoader, BEADataPipeline

class TestBEADataPipeline(unittest.TestCase):
    def setUp(self):
        self.api_key = 'TEST_API_KEY'
        self.sample_raw_data = {
            "BEAAPI": {
                "Results": [{

                    "Data": [
                        {
                            "TableName": "GDP",
                            "Industry": "GDP",  # Add the 'Industry' column
                            "Year": "2020",
                            "DataValue": "21000.0"
                        },
                        {
                            "TableName": "GDP",
                            "Industry": "Manufacturing",
                            "Year": "2020",
                            "DataValue": "5000.0"
                        }
                    ]
                }]
            }
        }
        self.sample_transformed_data = pd.DataFrame({
            "Line": [1, 2],
            "Description": ["GDP", "Manufacturing"],
            "2020": [21000.0, 5000.0]
        })

    def test_data_extractor(self):
        """Test DataExtractor can extract data"""
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = self.sample_raw_data
            mock_get.return_value = mock_response

            extractor = DataExtractor(self.api_key)
            result = extractor.extract()
            
            self.assertIsNotNone(result)
            self.assertEqual(result, self.sample_raw_data)

    
    def test_data_transformer(self):
        """Test DataTransformer can transform data"""
        # Update the mock data to match the structure and columns of the transformed data
        self.sample_raw_data = {
            "BEAAPI": {
                "Results": [{
                    "Data": [
                        {
                            "TableID": "1",
                            "Frequency": "A",
                            "Year": "2010",
                            "Quarter": "2010",
                            "Industry": "11",
                            "IndustryDescription": "Farms",
                            "DataValue": "145.7",
                            "NoteRef": "1"
                        },
                        {
                            "TableID": "1",
                            "Frequency": "A",
                            "Year": "2011",
                            "Quarter": "2011",
                            "Industry": "111CA",
                            "IndustryDescription": "Farms",
                            "DataValue": "117.0",
                            "NoteRef": "1"
                        },
                        {
                            "TableID": "1",
                            "Frequency": "A",
                            "Year": "2012",
                            "Quarter": "2012",
                            "Industry": "111CA",
                            "IndustryDescription": "Farms",
                            "DataValue": "117.0",
                            "NoteRef": "1"
                        },
                        {
                            "TableID": "1",
                            "Frequency": "A",
                            "Year": "2013",
                            "Quarter": "2013",
                            "Industry": "111CA",
                            "IndustryDescription": "Farms",
                            "DataValue": "117.0",
                            "NoteRef": "1"
                        },
                        {
                            "TableID": "1",
                            "Frequency": "A",
                            "Year": "2014",
                            "Quarter": "2014",
                            "Industry": "111CA",
                            "IndustryDescription": "Farms",
                            "DataValue": "117.0",
                            "NoteRef": "1"
                        },
                        {
                            "TableID": "1",
                            "Frequency": "A",
                            "Year": "2015",
                            "Quarter": "2015",
                            "Industry": "111CA",
                            "IndustryDescription": "Farms",
                            "DataValue": "117.0",
                            "NoteRef": "1"
                        },
                        {
                            "TableID": "1",
                            "Frequency": "A",
                            "Year": "2016",
                            "Quarter": "2016",
                            "Industry": "111CA",
                            "IndustryDescription": "Farms",
                            "DataValue": "117.0",
                            "NoteRef": "1"
                        },
                        {
                            "TableID": "1",
                            "Frequency": "A",
                            "Year": "2017",
                            "Quarter": "2017",
                            "Industry": "111CA",
                            "IndustryDescription": "Farms",
                            "DataValue": "117.0",
                            "NoteRef": "1"
                        },
                        {
                            "TableID": "1",
                            "Frequency": "A",
                            "Year": "2018",
                            "Quarter": "2018",
                            "Industry": "111CA",
                            "IndustryDescription": "Farms",
                            "DataValue": "117.0",
                            "NoteRef": "1"
                        },
                        {
                            "TableID": "1",
                            "Frequency": "A",
                            "Year": "2019",
                            "Quarter": "2019",
                            "Industry": "111CA",
                            "IndustryDescription": "Farms",
                            "DataValue": "117.0",
                            "NoteRef": "1"
                        },
                        {
                            "TableID": "1",
                            "Frequency": "A",
                            "Year": "2020",
                            "Quarter": "2020",
                            "Industry": "111CA",
                            "IndustryDescription": "Farms",
                            "DataValue": "117.0",
                            "NoteRef": "1"
                        },
                    ]
                }]
            }
        }

        # Instantiate the transformer and apply the transform method
        transformer = DataTransformer()
        transformed_df, title, last_revised = transformer.transform(self.sample_raw_data)

        # Assertions to validate the transformation
        self.assertEqual(title, "Value Added by Industry")  # Check the title matches
        self.assertEqual(last_revised, "Last Revised on: December 04, 2024")  # Check the last revised date
        self.assertFalse(transformed_df.empty)  # Ensure the transformed DataFrame is not empty

        # Check columns
        expected_columns = [
            "Line", "Description", "2010", "2011", "2012", "2013", "2014", "2015",
            "2016", "2017", "2018", "2019", "2020"
        ]
        self.assertListEqual(list(transformed_df.columns), expected_columns)

        # Check specific data values
        # self.assertEqual(
        #     transformed_df.loc[transformed_df["Description"] == "Farms", "2010"].values[0],
        #     145.7
        # )
        # self.assertEqual(
        #     transformed_df.loc[transformed_df["Description"] == "Farms", "2011"].values[0],
        #     117.0
        # )
        # self.assertEqual(
        #     transformed_df.loc[transformed_df["Description"] == "Farms", "2020"].values[0],
        #     117.0
        # )


    def test_data_loader(self):
        """Test DataLoader can save data"""
        loader = DataLoader()
        mock_data = (
            self.sample_transformed_data,
            "Test Title",
            "Test Revised Date"
        )
        
        with patch('os.makedirs'), patch('pandas.DataFrame.to_csv'):
            try:
                loader.load(mock_data)
            except Exception as e:
                self.fail(f"DataLoader raised {e}")

    def test_pipeline_run(self):
        """Test BEADataPipeline can run"""
        with patch.object(DataExtractor, 'extract', return_value=self.sample_raw_data), \
             patch.object(DataTransformer, 'transform', return_value=(self.sample_transformed_data, "Title", "Revised Date")), \
             patch.object(DataLoader, 'load'):
            
            pipeline = BEADataPipeline(self.api_key)
            try:
                pipeline.run()
            except Exception as e:
                self.fail(f"Pipeline run failed with {e}")

if __name__ == '__main__':
    unittest.main()
