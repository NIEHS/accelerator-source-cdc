import logging

from accelerator_core.workflow.accel_source_ingest import IngestSourceDescriptor
from cdc_accel_source import CDCAccelSource
from cdc_crosswalk import CDCCrosswalk
from cdc_metadata import CDCMetadata

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(filename)s:%(funcName)s:%(lineno)d: %(message)s"
)

logger = logging.getLogger(__name__)


def main(api_url: str, params: dict, type: str, submitter_name: str, submitter_email: str):
    # Create an IngestSourceDescriptor instance and populate metadata
    ingest_source_descriptor = IngestSourceDescriptor()
    ingest_source_descriptor.type = type
    ingest_source_descriptor.submitter_name = submitter_name
    ingest_source_descriptor.submitter_email = submitter_email
    ingest_source_descriptor.submit_date = '2021-04-04'

    # Initialize the specific ingest component
    cdc_accel_source = CDCAccelSource(ingest_source_descriptor)

    # Ingest the data
    ingest_results = cdc_accel_source.ingest({'api_url': api_url, 'params': params})
    
    # Perform data transformation and ingestion into a repository
    loop_datasets = 0
    #instance_metadata = CDCMetadata()
    #print(f"*****************{instance_metadata.api_field_name}")
    for entry in ingest_results.payload:
        abstruct_json_field(entry)
        #loop_datasets += 1
        #logger.info("Processing entry: %d", loop_datasets)
        #logger.info("Processing entry: %s", entry)
        #pass
    
    # Transform the data using a crosswalk
    crosswalk = CDCCrosswalk()
    #for doc in ingest_results:
        #ingest_result = crosswalk.transform
        #pass

def abstruct_json_field(data: dict) -> 'CDCMetadata':
    instance = CDCMetadata()

    # Mapping from expected field names to possible paths in the JSON object
    mappings = {
        #'name': data.get('name'),
        'resource_type': data.get('attribution'),
        #'homepage': data.get('customFields', {}).get('Common Core', {}).get('Homepage'),
        #'description': data.get('description'),
        #'category': data.get('category'),
        #'publisher': data.get('customFields', {}).get('Common Core', {}).get('Publisher'),
        #'tags': ', '.join(data.get('tags', [])) if data.get('tags') else '',
        #'data_created_date': data.get('createdAt'),
        #'data_updated_date': data.get('dataUpdatedAt'),
        #'suggested_citation': data.get('customFields', {}).get('Data Quality', {}).get('Suggested Citation'),
        #'update_frequency': data.get('customFields', {}).get('Common Core', {}).get('Update Frequency'),
        #'license': data.get('license'),
        #'source_link': data.get('webUri'),
        #'temporal_resolution': data.get('customFields', {}).get('Common Core', {}).get('Temporal Applicability'),
        #'geopatial_resolution': data.get('customFields', {}).get('Data Quality', {}).get('Geospatial Resolution'),
        #'geographic_coverage': data.get('customFields', {}).get('Common Core', {}).get('Geographic Coverage'),
        #'geographic_unit_of_analysis': data.get('customFields', {}).get('Data Quality', {}).get('Geographic Unit of Analysis'),
        #'column_name': '',  # Not available in this structure
        
        #'api_field_name': '',  # Not available in this structure
        #'data_type': '',  # Not available in this structure
    }

    for field, value in mappings.items():
        if hasattr(instance, field):
            setattr(instance, field, str(value) if value is not None else '')
    print(f"*****************{instance.attribution}")
    return instance


if __name__ == '__main__':
    api_url = "https://data.cdc.gov/api/views/metadata/v1"
    params = {
        "rows": 10,  # Maximum per request (adjust if needed)
        "start": 0  # Start at 0 and increase in increments of `rows`
    }
    type = 'CHORDS'
    submitter_name = 'John Doe'
    submitter_email = 'john.doe@test.com'
    main(api_url=api_url, params=params, type=type, submitter_name=submitter_name, submitter_email=submitter_email)
