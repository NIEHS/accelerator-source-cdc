
from accelerator_core.workflow.accel_source_ingest import AccelIngestComponent
from accelerator_core.workflow.crosswalk import Crosswalk

from accelerator_core.schema.models.accel_model import (
    AccelProgramModel,
    AccelProjectModel,
    AccelIntermediateResourceModel,
    AccelResourceReferenceModel,
    AccelResourceUseAgreementModel,
    AccelPublicationModel,
    AccelDataResourceModel,
    AccelDataLocationModel,
    AccelGeospatialDataModel,
    AccelTemporalDataModel,
    AccelPopulationDataModel,
)

from accelerator_core.schema.models.base_model import (
    SubmissionInfoModel,
    TechnicalMetadataModel,
)

class CDCCrosswalk(Crosswalk):
    """
    Crosswalks data from the ingest result into the appropriate format for downstream processing.
    """

    def transform(self, ingest_result: AccelIngestComponent) -> AccelIngestComponent :
    #def transform(self, ingest_result: dict) -> dict:
        # For demonstration, let's assume the transformation just returns the payload directly
        #print(f"Transforming ingest result: {ingest_result.payload}")
        crosswalk_payload = []
        for cdc_single in ingest_result.payload:
            print(f"CDC name before cross walk : {cdc_single.get('resource', {}).get('name', {})}")
            crosswalk_entry = CDCCrosswalk.transform_to_model(self,cdc_single = cdc_single)
            crosswalk_payload.append(crosswalk_entry)   
        
        ingest_result.payload = crosswalk_payload
            # Perform transformation logic here
            # For example, you might want to modify the entry or extract specific fields
            # entry['transformed_field'] = entry.get('original
        print("Done Crosswalk")
        return ingest_result
    @staticmethod
    def transform_to_model(self, cdc_single:dict) -> TechnicalMetadataModel:
        """
        Transform the ingest result into a TechnicalMetadataModel.
        Populate the model with data from the ingest result 
        """
        # Create an instance of the SubmissionInfoModel
        
        submission_info_model = SubmissionInfoModel()               
        submission_info_model.submitter_name = cdc_single.get('submitter', {}).get('contact name', {})
        submission_info_model.submitter_email = cdc_single.get('submitter', {}).get('contact email', {})
        submission_info_model.submit_date = cdc_single["resource"].get("name")
        #*** need model provide mapping for the below fields
        # Author info
        data_owner = cdc_single["author"].get("data owner")
        data_provider = cdc_single["author"].get("data provided by")

                 
        program = AccelProgramModel()
        program.code = "??code"
        program.name = "??name"
        program.preferred_label = "??preferred_label"

        project = AccelProjectModel()
        project.code = "code"
        project.name = "name"
        project.project_sponsor = ["sponsor1"]
        project.project_sponsor_other = ["sponsor2"]
        project.project_sponsor_type = ["sponsor_type1"]
        project.project_sponsor_type_other = ["sponsor_type2"]
        project.project_ur = "http://project.url.com"

        mediate_resource = AccelIntermediateResourceModel()
        mediate_resource.name = cdc_single["resource"].get("name")
        mediate_resource.description = cdc_single["resource"].get("description")
        mediate_resource.homepage = cdc_single["resource"].get("Homepage")
        mediate_resource.category = cdc_single["resource"].get("category")
        mediate_resource.publisher = cdc_single["resource"].get("publisher")
        mediate_resource.tags = cdc_single["resource"].get("tags")
        mediate_resource.data_created_date = cdc_single["resource"].get("data created date")
        mediate_resource.data_updated_date = cdc_single["resource"].get("data updated date")
        mediate_resource.suggested_citation = cdc_single["resource"].get("Suggested Citation")
        mediate_resource.license = cdc_single["resource"].get("license")
        mediate_resource.resource_url = cdc_single["resource reference"].get("source link")
        
        publication = AccelPublicationModel()
        publication.citation = cdc_single["resource"].get("Suggested Citation")
        publication.citation_link = cdc_single["resource reference"].get("source link")
        
        resource_reference = AccelResourceReferenceModel()
        resource_reference.resource_reference_text = cdc_single["resource reference"].get("source link")
        resource_reference.resource_reference_link = cdc_single["resource reference"].get("source link")
   

        geospatial = AccelGeospatialDataModel()
        geospatial.spatial_coverage = cdc_single["geospatial data"].get("Geographic Coverage")
        geospatial.spatial_coverage_other = cdc_single["geospatial data"].get("Geographic Unit of Analysis")
        geospatial.geospatial_resolution = cdc_single["geospatial data"].get("Geopatial Resolution")

        #return submission_info_model
        return mediate_resource