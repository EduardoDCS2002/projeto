import upload_to_openmetadata
import checkingckan
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

def ckan_to_om():
    results = {"created": 0, "skipped": 0, "failed": 0}
    datasets = checkingckan.get_all_datasets_from_ckan(CKAN_URL, API_KEY)
    for org,dsets in datasets.items():
        for dataset in dsets:
            dcat = convertors.ckan_to_my_dcat(dataset)
            try:
                upload_to_openmetadata.create_dataset_table(TOKEN, dcat)
                results["created"] += 1
            except Exception as e:
                if "Table exists" in str(e):
                    results["skipped"] += 1
                else:
                    print(f"Error: {str(e)}")
                    results["failed"] += 1
    print("\nResults:")
    print(f"Created: {results['created']}, Skipped: {results['skipped']}, Failed: {results['failed']}")


if __name__ == "__main__":
    ckan_to_om()