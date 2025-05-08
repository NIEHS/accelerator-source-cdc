
from typing import List
import json

from cdc_metadata import CDCMetadata

class Process_CDC_json:
    
    @staticmethod
    def extract_metadata_from_json(record:dict) -> dict:
        """
        Extract metadata from a JSON file.
        :param json_record: 
        :return: Dictionary containing the extracted metadata
        """      
        
        #result = []
        #for record in data:
        custom_fields = record.get("customFields", {}) or {}
        core = custom_fields.get("Common Core", {}) or {}
        quality = custom_fields.get("Data Quality", {}) or {}

        entry = {
            "submitter": {
                "contact name": core.get("Contact Name"),
                "contact email": core.get("Contact Email")
            },
            "author": {
                "data owner": core.get("Contact Name"),
                "data provided by": core.get("Publisher")
            },
            "resource": {
                "name": record.get("name"),
                "Homepage": core.get("Homepage"),
                "description": record.get("description"),
                "category": record.get("category"),
                "publisher": core.get("Publisher"),
                "tags": record.get("tags"),
                "data created date": record.get("createdAt"),
                "data updated date": record.get("dataUpdatedAt"),
                "Suggested Citation": quality.get("Suggested Citation")
            },
            "resource reference": {
                "Update frequency": core.get("Update Frequency"),
                "source link": record.get("webUri")
            },
            "geospatial data": {
                "Geopatial Resolution": quality.get("Geospatial Resolution"),
                "Geographic Coverage": core.get("Geographic Coverage"),
                "Geographic Unit of Analysis": quality.get("Geographic Unit of Analysis")
            }
        }
            #result.append(entry)
        return entry

    #@staticmethod
    #def from_json_file(json_file_path: str) -> List['CDCMetadata']:
        #with open(json_file_path, 'r') as f:
            #raw_data = json.load(f)
        #return [CDCMetadata.from_json_object(entry) for entry in raw_data]
    #This function now directly reads the JSON file and creates a list of CDCMetadata objects, 
    # each containing the extracted files.
    @staticmethod
    def create_cdc_metadata_list(json_file_path: str) -> List['CDCMetadata']:
        cdc_metadata_list = []
        try:
            with open(json_file_path, 'r') as f:
                raw_data_list = json.load(f)
                if isinstance(raw_data_list, list):
                    for entry in raw_data_list:                      
                        cdc_metadata_list.append(Process_CDC_json.extract_metadata_from_json(entry))
                else:
                    # Handle the case where the JSON file contains a single object, not a list                                     
                    cdc_metadata_list.append(Process_CDC_json.extract_metadata_from_json(raw_data_list))
                    
        except FileNotFoundError:
            print(f"Error: File not found at {json_file_path}")
        except json.JSONDecodeError:
            print(f"Error: Could not decode JSON from {json_file_path}")
        return cdc_metadata_list
    
    @staticmethod
    def from_json_object(entry: dict) -> 'CDCMetadata':
        cdc_instance = CDCMetadata()       
       
        cdc_instance.name = CDCMetadata.get_name(entry)
        cdc_instance.homepage = CDCMetadata.get_homepage(entry)
        cdc_instance.resource_type = CDCMetadata.get_resource_type(entry)        
        '''
        instance.description = data.get('description')
        instance.category = data.get('category')
        instance.publisher = data.get('customFields', {}).get('Common Core', {}).get('Publisher')
        instance.tags = ', '.join(data.get('tags', [])) if data.get('tags') else ''
        instance.data_created_date = data.get('createdAt')
        instance.data_updated_date = data.get('dataUpdatedAt')
        instance.suggested_citation = data.get('customFields', {}).get('Data Quality', {}).get('Suggested Citation')
        instance.update_frequency = data.get('customFields', {}).get('Common Core', {}).get('Update Frequency')
        instance.license = data.get('license')
        instance.source_link = data.get('webUri')
        instance.temporal_resolution = data.get('customFields', {}).get('Common Core', {}).get('Temporal Applicability')
        instance.geopatial_resolution = data.get('customFields', {}).get('Data Quality', {}).get('Geospatial Resolution')
        instance.geographic_coverage = data.get('customFields', {}).get('Common Core', {}).get('Geographic Coverage')
        instance.geographic_unit_of_analysis = data.get('customFields', {}).get('Data Quality', {}).get('Geographic Unit of Analysis')
        '''      


        '''for field, value in mappings.items():
            if hasattr(instance, field):
                setattr(instance, field, str(value) if value is not None else '')
       '''
        return cdc_instance