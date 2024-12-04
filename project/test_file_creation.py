import os
import unittest
from datetime import datetime
from run_etl import run_all_pipelines

class TestPipelineFileCreation(unittest.TestCase):
    def test_pipeline_file_creation(self):
        """
        Test that pipelines create files in the data directory
        """
        # Get the data directory path
        current_file_path = os.path.abspath(__file__)
        project_root = os.path.dirname(os.path.dirname(current_file_path))
        data_dir = os.path.join(project_root, 'data')

        # Ensure data directory exists
        os.makedirs(data_dir, exist_ok=True)

        # Run all pipelines
        run_all_pipelines()

        # Get today's date for filename matching
        today_date = datetime.now().strftime('%Y%m%d')

        # Check for emissions data file
        emissions_files = [f for f in os.listdir(data_dir) 
                           if f.startswith('ghg_emissions_') and f.endswith('.csv')]
        self.assertTrue(len(emissions_files) > 0, "No emissions data file created")
        
        # Check for BEA data file
        bea_files = [f for f in os.listdir(data_dir) 
                     if f.startswith('gdp_by_industry') and f.endswith('.csv')]
        self.assertTrue(len(bea_files) > 0, "No BEA data file created")

        # Optional: Verify file sizes are not empty
        for filename in emissions_files + bea_files:
            file_path = os.path.join(data_dir, filename)
            self.assertTrue(os.path.getsize(file_path) > 0, f"{filename} is empty")

if __name__ == '__main__':
    unittest.main()