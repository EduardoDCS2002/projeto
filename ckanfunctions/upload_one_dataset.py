import upload_dataset_to_ckan
import requests

CKAN_URL="http://localhost:5000/api/3/action/"
API_KEY="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJtSDF4V2pmQkEtMXRJZXNMRGZTaEk4U0dPX0JfdEtaVi1jSHBIeVJfcTJRIiwiaWF0IjoxNzQ0Mjk4MTIyfQ.7yBFMAk28YDFdq39PHnHPPJDaQxtRWxmKsiU0p4x6cc"



# needs to be given the source url and the dataset name
result = upload_dataset_to_ckan.sync_dataset(
    source_url="https://catalog.data.gov/api/3/action/",
    target_ckan_url=CKAN_URL,
    target_api_key=API_KEY,
    dataset_name="inventory-of-owned-and-leased-properties-iolp"
    )

print(result)
# Output:
# {
#   'status': 'success',
#   'dataset_id': 'climate-data',
#   'resources_added': ['http://source.gov/data1.csv', ...],
#   'resources_skipped': ['http://source.gov/existing.csv'], 
#   'errors': []
# }
