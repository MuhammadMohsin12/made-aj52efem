from emissions_etl import EmissionsDataPipeline
from bea_etl import BEADataPipeline

def run_all_pipelines():
    """Run all available data pipelines"""
    
    print("=== Starting Emissions Data Pipeline ===")
    emissions_pipeline = EmissionsDataPipeline()
    emissions_pipeline.run()
    print("\n")
    
    print("=== Starting BEA Data Pipeline ===")
    bea_pipeline = BEADataPipeline(api_key='2B9CC899-D595-4CE0-9E7D-E555717EF787')
    bea_pipeline.run()

if __name__ == "__main__":
    run_all_pipelines()