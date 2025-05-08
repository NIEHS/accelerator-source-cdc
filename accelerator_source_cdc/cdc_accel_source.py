import requests
import logging
import uuid
import json
import os
from datetime import datetime


from accelerator_core.workflow.accel_source_ingest import AccelIngestComponent
from accelerator_core.workflow.accel_source_ingest import IngestSourceDescriptor
#from accelerator_core.workflow.accel_source_ingest import IngestResult
from accelerator_core.workflow.accel_source_ingest import IngestPayload
from cdc_process_json import Process_CDC_json

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(filename)s:%(funcName)s:%(lineno)d: %(message)s"

)
logger = logging.getLogger(__name__)

class CDCAccelSource(AccelIngestComponent):
    """
    A subclass of AccelIngestComponent that implements the ingest process for CDC data.
    This class is responsible for fetching data from the CDC API and processing it into a format suitable for ingestion.
    It uses the IngestSourceDescriptor to define the source of the data and the IngestResult to encapsulate the result of the ingestion process.
    The ingest method is the main entry point for the ingestion process, and it uses the basic_dataset_search method to fetch data from the CDC API.
    The dump_data method is used to save the fetched data into JSON files in a specified folder.
    The class also includes a static method for basic dataset search, which can be used to fetch datasets from the CDC API.
    The class is designed to be extensible, allowing for additional functionality to be added in the future.
    It is important to note that this class is part of a larger framework and is intended to be used in conjunction with other components of the framework.
    """
    def __init__(self, ingest_source_descriptor: IngestSourceDescriptor):
        super().__init__(ingest_source_descriptor)

    #def ingest(self, additional_parameters: dict) -> IngestSourceDescriptor:
    def ingest(self, additional_parameters: dict) -> AccelIngestComponent:
        """
        Ingest data from a data.gov and return the result in an IngestResult object.

        :param additional_parameters: Dictionary containing parameters such as the api url and token
        :return: IngestResult with the parsed data from the spreadsheet
        """
        cdc_metadatesets = []

        api_url = additional_parameters.get('api_url')
        params = additional_parameters.get('params')
        # Load local json file if api_url is not provided
        json_file = "./tests/test_resources/cdc_dump_04_21_2025.json"
        if not api_url or not params:
            #raise ValueError("API URL and parameters must be provided")
            logger.info("!!!!Load CDC metadata json from local.")
            cdc_metadatesets = Process_CDC_json.create_cdc_metadata_list(json_file)
        else:
            # Call the basic dataset search method
            logger.info("!!!!Load CDC metadata json from URL: %s",{api_url})
            cdc_metadatesets = CDCAccelSource.fetch_cdc_metadata(api_url=api_url, params=params)
        
        # Get the result datasets    
        if not cdc_metadatesets:
            logger.info("No results found.")
            return None
        count = len(cdc_metadatesets)
        #datasets = query_result.get("datasets", [])

        logger.info("Datasets found: %s", {count})
        if count < 1:
            return None
        else:
            # Create an AccelIngestComponent object
            ingest_result = AccelIngestComponent(self.ingest_source_descriptor)
            ingest_result.payload = cdc_metadatesets
            ingest_result.ingest_successful = True
            return ingest_result
        

    @staticmethod
    def fetch_cdc_metadata(api_url: str = None, params: dict = None, rows: int = 1000) -> dict:
        
        metadata = []
        if not api_url or not params:
            logger.info("API URL and parameters must be provided.")
            return None
        base_folder = "./tests/test_resources"
        #I download the cdc data at 04_21_2025 for the purpose of development
        json_file = "./tests/test_resources/cdc_dump_04_21_2025.json" 

        """
        Dumps CDC dataset metadata to a dated subfolder.
        Args:
            metadata (list): List of dataset metadata dictionaries.
            base_folder (str): Parent directory to place the dump folder in.

        Returns:
            str: Full path to the created dump folder.
        """
        date_str = datetime.today().strftime("%m_%d_%Y")
        file_name = f"cdc_dump_{date_str}.json"
        file_path = os.path.join(base_folder, file_name)
        os.makedirs(base_folder, exist_ok=True)
        print(f"Folder created: {file_path}")


        # Check if the json file is exist if not download from CDC currently just check the directory dont need down load the datase each time during the development
        if not os.path.exists(file_path):
            logger.info(f"***File does not exist: {file_path}, Loading from API.")
            
            response = requests.get(api_url)
            if response.status_code == 200:
                metadata = response.json()
                # Save metadata to a JSON file
                json_file = CDCAccelSource.dump_cdc_meta(metadata)
             
            else:
                print(f"Failed to fetch metadata. Status code: {response.status_code}")
        else:
            # If the file exists, read the metadata from it
            with open(json_file, "r") as f:
                metadata = json.load(f)
            logger.info(f"***Metadata loaded from {json_file}.")
            count =len(metadata)
            logger.info("***Retrieved metadata for %d",  count )
        return metadata
            
            

    @staticmethod
    def dump_cdc_meta (metadata: list, file_path: str = "./tests/test_resources") -> str:
        
        with open(file_path, "w") as f:
            json.dump(metadata, f, indent=2)
        return file_path


    @staticmethod
    def basic_dataset_search(api_url: str = None, params: dict = None, rows: int = 1000) -> dict:
        """
        Get JSON-formatted lists of data.gov site’s datasets
        """

        if not api_url or not params:
            logger.info("API URL and parameters must be provided.")
            return None
    @staticmethod
    def dump_data(datasets: list):
        """
        Dump the data into JSON files in a specified folder.
        :param datasets: A list of datasets
        :return:
        """
        # Folder containing JSON files
        folder_path = "../tests/test_resources/cdc_dump_04_10_2025"
        MAX_FILENAME_LENGTH = 20  # Set max length for the dataset title
        file_count = 0
        file_name_list = []
        # Check if the folder exists, if not, create it
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            logger.info(f"Folder created: {folder_path}")
        else:
            logger.info(f"Folder already exists: {folder_path}")