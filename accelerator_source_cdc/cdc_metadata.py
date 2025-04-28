class CDCMetadata:
    def __init__(self):
        self.name = ""
        self.resource_type = ""
        self.homepage = ""
        self.description = ""
        self.category = ""
        self.publisher = ""
        self.tags = ""
        self.data_created_date = "created_date"
        self.data_updated_date = ""
        self.suggested_citation = ""
        self.update_frequency = ""
        self.license = ""
        self.source_link = ""
        self.temporal_resolution = ""
        self.geopatial_resolution = ""
        self.geographic_coverage = ""
        self.geographic_unit_of_analysis = ""
        self.column_name = ""
        self.api_field_name = "test api_field_name"
        self.data_type = ""
    
    def get_name(entry:dict)->str:
        """return the name of the dataset"""
        # Check if the entry has a 'name' field
        if 'name' in entry:
            #return entry['name']
            return entry.get('name')
        else:
            # If not, return an empty string or handle as needed
            return ''
        
    def get_resource_type(entry:dict)->str:
        """return the resource type of the dataset"""
        # Check if the entry has a 'resource_type' field
        if 'resource_type' in entry:
            return entry['resource_type']
        else:
            # If not, return an empty string or handle as needed
            return ''
    def get_homepage(entry:dict)->str:
        """return the homepage of the dataset"""
        custom_fields = entry.get('customFields')

        common_core = custom_fields.get('Common Core') if custom_fields else None
        homepage = common_core.get('Homepage') if common_core else ''

        if homepage is None:
            homepage = ''
        return homepage
    def get_category(entry:dict)->str:
        """return the category of the dataset"""
        # Check if the entry has a 'category' field
        if 'category' in entry:
            return entry['category']
        else:
            # If not, return an empty string or handle as needed
            return ''
    def get_publisher(entry:dict)->str:
        """return the publisher of the dataset"""
        custom_fields = entry.get('customFields')
        common_core = custom_fields.get('Common Core') if custom_fields else None
        publisher = common_core.get('Publisher') if common_core else ''

        if publisher is None:
            publisher = ''
        return publisher
    def get_tags(entry:dict)->str:
        """return the tags of the dataset"""
        # Check if the entry has a 'tags' field
        if 'tags' in entry:
            return ', '.join(entry['tags'])
        else:
            # If not, return an empty string or handle as needed
            return ''
    def get_data_created_date(entry:dict)->str:
        """return the data created date of the dataset"""
        # Check if the entry has a 'data_created_date' field
        if 'data_created_date' in entry:
            return entry['data_created_date']
        else:
            # If not, return an empty string or handle as needed
            return ''               
    def get_data_updated_date(entry:dict)->str:
        """return the data updated date of the dataset"""
        # Check if the entry has a 'data_updated_date' field
        if 'data_updated_date' in entry:
            return entry['data_updated_date']
        else:
            # If not, return an empty string or handle as needed
            return ''
    def get_suggested_citation(entry:dict)->str:    
        """return the suggested citation of the dataset"""
        custom_fields = entry.get('customFields')
        data_quality = custom_fields.get('Data Quality') if custom_fields else None
        suggested_citation = data_quality.get('Suggested Citation') if data_quality else ''

        if suggested_citation is None:
            suggested_citation = ''
        return suggested_citation
    def get_update_frequency(entry:dict)->str:  
        """return the update frequency of the dataset"""
        custom_fields = entry.get('customFields')
        common_core = custom_fields.get('Common Core') if custom_fields else None
        update_frequency = common_core.get('Update Frequency') if common_core else ''

        if update_frequency is None:
            update_frequency = ''
        return update_frequency             
    def get_license(entry:dict)->str:
        """return the license of the dataset"""
        # Check if the entry has a 'license' field
        if 'license' in entry:
            return entry['license']
        else:
            # If not, return an empty string or handle as needed
            return ''
    def get_source_link(entry:dict)->str:
        """return the source link of the dataset"""
        # Check if the entry has a 'source_link' field
        if 'source_link' in entry:
            return entry['source_link']
        else:
            # If not, return an empty string or handle as needed
            return ''
    def get_temporal_resolution(entry:dict)->str:   
        """return the temporal resolution of the dataset"""
        custom_fields = entry.get('customFields')
        common_core = custom_fields.get('Common Core') if custom_fields else None
        temporal_resolution = common_core.get('Temporal Applicability') if common_core else ''

        if temporal_resolution is None:
            temporal_resolution = ''
        return temporal_resolution
    def get_geopatial_resolution(entry:dict)->str:
        """return the geopatial resolution of the dataset"""
        custom_fields = entry.get('customFields')
        data_quality = custom_fields.get('Data Quality') if custom_fields else None
        geopatial_resolution = data_quality.get('Geospatial Resolution') if data_quality else ''

        if geopatial_resolution is None:
            geopatial_resolution = ''
        return geopatial_resolution
    def get_geographic_coverage(entry:dict)->str:
        """return the geographic coverage of the dataset"""
        custom_fields = entry.get('customFields')
        common_core = custom_fields.get('Common Core') if custom_fields else None
        geographic_coverage = common_core.get('Geographic Coverage') if common_core else ''

        if geographic_coverage is None:
            geographic_coverage = ''
        return geographic_coverage
    #def get_geographic_unit_of_analysis(entry:dict)->str: