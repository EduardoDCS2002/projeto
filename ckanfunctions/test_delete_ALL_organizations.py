import requests
from urllib.parse import urljoin

CKAN_URL = "http://localhost:5000/api/3/action/"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJtSDF4V2pmQkEtMXRJZXNMRGZTaEk4U0dPX0JfdEtaVi1jSHBIeVJfcTJRIiwiaWF0IjoxNzQ0Mjk4MTIyfQ.7yBFMAk28YDFdq39PHnHPPJDaQxtRWxmKsiU0p4x6cc"

def list_all_datasets(ckan_url, orgname, api_key=None, include_private=True):
    #List all datasets even hidden
    params = {
        "organization": orgname,
        "rows": 1000,  # Max allowed by CKAN
        "include_private": include_private
    }
    response = requests.get(
        urljoin(ckan_url, "package_search"),
        headers={"Authorization": api_key},
        params=params,
        timeout=15
    )
    response.raise_for_status()
    data = response.json()
    
    if not data.get("success"):
        raise ValueError(f"API error: {data.get('error')}")
    
    return [pkg["name"] for pkg in data["result"]["results"]]

def delete_dataset(ckan_url, dataset_id_or_name, api_key): # It was necessary to fix an old error
    # Delete a dataset from CKAN
    try:
        response = requests.post(
            urljoin(ckan_url, "package_delete"),
            headers={"Authorization": api_key},
            json={"id": dataset_id_or_name},
            timeout=15
        )
        response.raise_for_status()
        data = response.json()
        
        if not data.get("success"):
            raise ValueError(f"API error: {data.get('error', 'Unknown error')}")
        
        print(f"Successfully deleted dataset: {dataset_id_or_name}")
        return data["result"]
    
    except requests.exceptions.RequestException as e:
        raise ValueError(f"Failed to delete dataset: {str(e)}")



def get_all_organizations():
    # Get all organizations including deleted ones
    response = requests.post(
        f"{CKAN_URL}organization_list",
        headers={"Authorization": API_KEY},
        json={"all_fields": True, "include_deleted": True}
    )
    return response.json().get("result", [])

def purge_organization(name):
    # Completely remove an organization
    response = requests.post(
        f"{CKAN_URL}organization_purge",
        headers={"Authorization": API_KEY},
        json={"id": name}
    )
    return response.json()

def purge_all_organizations():
    # Purge all organizations from the system
    orgs = get_all_organizations()
    print(f"Found {len(orgs)} organizations to purge")
    
    results = []
    
    for org in orgs:
        org_name = org.get("name")
        if not org_name:
            continue
            
        print(f"\nPurging organization: {org_name}")

        datasetsnames = list_all_datasets(CKAN_URL, org_name, API_KEY)

        for datasetname in datasetsnames:
            delete_dataset(CKAN_URL, datasetname, API_KEY)
        
        try:
            result = purge_organization(org_name)
            results.append({
                "name": org_name,
                "success": result.get("success", False),
                "error": result.get("error")
            })
        except Exception as e:
            results.append({
                "name": org_name,
                "success": False,
                "error": str(e)
            })
    print(results)
    return results
purge_all_organizations()