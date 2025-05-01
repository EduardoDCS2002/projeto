import upload_dataset
import checkopenmetadata
import convertors
import requests
import base64

CKAN_URL = "http://localhost:5000/api/3/action/"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJtSDF4V2pmQkEtMXRJZXNMRGZTaEk4U0dPX0JfdEtaVi1jSHBIeVJfcTJRIiwiaWF0IjoxNzQ0Mjk4MTIyfQ.7yBFMAk28YDFdq39PHnHPPJDaQxtRWxmKsiU0p4x6cc"

OPENMETADATA_URL = "http://localhost:8585/api/v1"
LOGIN_URL = f"{OPENMETADATA_URL}/users/login"

def get_auth_token():
    """Get OpenMetadata auth token"""
    headers = {"Content-Type": "application/json"}
    response = requests.post(
        LOGIN_URL,
        headers=headers,
        json={
            "email": "admin@open-metadata.org",
            "password": base64.b64encode("admin".encode()).decode()
        }
    )
    response.raise_for_status()
    return response.json()["accessToken"]

TOKEN = get_auth_token()

def om_to_ckan():
    
    tables = checkopenmetadata.get_schema_tables_dict()
    for table in tables:
        my_dcat = convertors.openmetadata_to_my_dcat(table)
        
        results = upload_dataset.sync_dataset(my_dcat, CKAN_URL, API_KEY)
        print(results)

if __name__ == "__main__":
    om_to_ckan()