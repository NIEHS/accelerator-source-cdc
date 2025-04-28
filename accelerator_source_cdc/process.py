import logging
from accelerator_core.workflow.accel_source_ingest import IngestSourceDescriptor

from cdc_accel_source import CDCAccelSource

from cdc_crosswalk import CDCCrosswalk
from accelerator_core.workflow.accel_source_ingest import IngestSourceDescriptor
from accelerator_core.workflow.accel_source_ingest import IngestPayload
from cdc_metadata import CDCMetadata
from cdc_process_json import Process_CDC_json



import os


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(filename)s:%(funcName)s:%(lineno)d: %(message)s"
)

logger = logging.getLogger(__name__)
def main(json_file: str = None, api_url: str = None, params: dict = None, type: str = None, submitter_name: str = None, submitter_email: str = None):

    # Create an IngestSourceDescriptor instance and populate metadata
    ingest_source_descriptor = IngestSourceDescriptor()
    ingest_source_descriptor.type = type
    ingest_source_descriptor.submitter_name = submitter_name
    ingest_source_descriptor.submitter_email = submitter_email
    ingest_source_descriptor.submit_date = '2021-04-04'
    ingest_source_descriptor.source_name = 'CDC'
    ingest_source_descriptor.source_type = 'API'
    ingest_source_descriptor.source_url = api_url
    ingest_source_descriptor.source_description = 'CDC API'

    cdc_accel_source = CDCAccelSource(ingest_source_descriptor)

    if json_file:
        # Use the existing source
        print(f"********Load CDC json_file from local = {json_file}")
        ingest_results = cdc_accel_source.ingest({'api_url': '', 'params': params})   
    elif api_url:
        # Use the API source
        print(f"**** CDC update metadata json URL : {api_url}")
        # Ingest the data
        ingest_results = cdc_accel_source.ingest({'api_url': api_url, 'params': params})
    else:
        print("Error: Neither 'json_file' nor 'api_url' was provided to main().")
 
    # Initialize the specific ingest component   
    # Perform data transformation and ingestion into a repository
    loop_datasets = 0
    instance_metadata = CDCMetadata()
    cdc_metadata_list = []
    if not ingest_results:
        print("No ingest results found.")
        return
    
    # Check the input items Loop through the ingest results
    #for entry in ingest_results.payload:
        #print(f"entry ******* %s",entry.get("resource", {}).get("name", {}))
        
        #logger.info("Processing entry: %d", loop_datasets)
        #logger.info("Processing entry: %s", entry)
        #pass
    
    # Transform the data using a crosswalk
    crosswalk = CDCCrosswalk()
    
    transform_ingest_result = crosswalk.transform(ingest_results)
    
    for doc in ingest_results.payload:
        #ingest_result = crosswalk.transform
        print(f"Transformed entry: %s", doc.submitter_name)
        #pass

def abstruct_json_field(data: dict) -> 'CDCMetadata':
    cdc_metadata_list = []
    instance = Process_CDC_json.from_json_object(data)   
     
    # Mapping from expected field names to possible paths in the JSON object
    mappings = {
        'name': data.get('name'),       
    }   
    return instance


if __name__ == '__main__':
    api_url = "https://data.cdc.gov/api/views/metadata/v1"
    # Use the existing source
    json_file = "./tests/test_resources/cdc_dump_04_21_2025.json"
    params = {
        "rows": 10,  # Maximum per request (adjust if needed)
        "start": 0  # Start at 0 and increase in increments of `rows`
    }
    type = 'CHORDS'
    submitter_name = 'John Doe'
    submitter_email = 'john.doe@test.com'
    IFUPDATESOURCE = False
    if IFUPDATESOURCE:
        # Update the source
        main(api_url=api_url, params=params, type=type, submitter_name=submitter_name, submitter_email=submitter_email)
    else:        
        main(json_file=json_file, params=params, type=type, submitter_name=submitter_name, submitter_email=submitter_email)

        
        
    
