import os
import re
from typing import List
import json


from cdc_metadata import CDCMetadata

class Process_CDC_json:
    @staticmethod
    def from_json_object(entry: dict) -> 'CDCMetadata':
        cdc_instance = CDCMetadata()

        # Mapping from expected field names to possible paths in the JSON object
               
        cdc_instance.name = CDCMetadata.get_name(entry)
        cdc_instance.homepage = CDCMetadata.get_homepage(entry)
        cdc_instance.resource_type = CDCMetadata.get_resource_type(entry)
        if  cdc_instance.homepage is not None and  cdc_instance.homepage != '':            
            print(f"*****************{cdc_instance.name}")
            print(f"&&&&&&&&homepage = {cdc_instance.homepage}")        
        
        #instance.column_name = '' # Not available in this structure
        # instance.api_field_name = data.get('api_field_name') # Not available in this structure
        # instance.data_type = data.get('data_type') # Not available in this structure

        '''for field, value in mappings.items():
            if hasattr(instance, field):
                setattr(instance, field, str(value) if value is not None else '')
'''
        return cdc_instance

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
                        cdc_metadata_list.append(Process_CDC_json.from_json_object(entry))
                else:
                    # Handle the case where the JSON file contains a single object, not a list                                     
                    cdc_metadata_list.append(Process_CDC_json.from_json_object(raw_data_list))
                    
        except FileNotFoundError:
            print(f"Error: File not found at {json_file_path}")
        except json.JSONDecodeError:
            print(f"Error: Could not decode JSON from {json_file_path}")
        return cdc_metadata_list

# Load and parse the JSON file
if __name__ == "__main__":
    json_file = "tests/test_resources/cdc_dump_04_21_2025.json"
    if os.path.exists(json_file):
        #metadata_list = CDCMetadata.from_json_file(json_file)
        metadata_list = Process_CDC_json.create_cdc_metadata_list(json_file)
        print(f"Loaded {len(metadata_list)} metadata entries.")
    else:
        print(f"File {json_file} does not exist.")