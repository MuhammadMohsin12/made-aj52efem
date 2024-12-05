# Install dependencies
pip install -r ../requirements.txt

#Run Test cases for the pipelinea
python -m unittest .\test_bea_etl.py
python -m unittest .\test_emissions_etl.py
python -m unittest .\test_file_creation.py