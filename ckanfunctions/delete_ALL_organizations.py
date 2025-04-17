import requests
from urllib.parse import urljoin, quote
import time

CKAN_URL = "http://localhost:5000/"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJtSDF4V2pmQkEtMXRJZXNMRGZTaEk4U0dPX0JfdEtaVi1jSHBIeVJfcTJRIiwiaWF0IjoxNzQ0Mjk4MTIyfQ.7yBFMAk28YDFdq39PHnHPPJDaQxtRWxmKsiU0p4x6cc"

def list_all_datasets(ckan_url, orgname, api_key=None):
    # List all datasets for an organization including private ones
    endpoint = urljoin(ckan_url, "api/3/action/package_search")
    
    params = {
        "q": f"organization:{quote(orgname)}",
        "rows": 1000,
        "include_private": True
    }
    
    try:
        response = requests.get(
            endpoint,
            headers={"Authorization": api_key},
            params=params,
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        
        if not data.get("success"):
            print(f"API Warning for {orgname}: {data.get('error', {}).get('message', 'Unknown error')}")
            return []
            
        return [pkg["name"] for pkg in data.get("result", {}).get("results", [])]
        
    except requests.exceptions.RequestException as e:
        print(f"Error listing datasets for {orgname}: {str(e)}")
        return []

def delete_dataset(ckan_url, dataset_id_or_name, api_key):
    # Delete a dataset from CKAN with retry logic
    endpoint = urljoin(ckan_url, "api/3/action/package_delete")
    
    for attempt in range(3):
        try:
            response = requests.post(
                endpoint,
                headers={"Authorization": api_key},
                json={"id": dataset_id_or_name},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            
            if not data.get("success"):
                raise ValueError(f"API error: {data.get('error', 'Unknown error')}")
            
            print(f"Successfully deleted dataset: {dataset_id_or_name}")
            return data["result"]
            
        except requests.exceptions.RequestException as e:
            if attempt == 2:  # Last attempt
                raise ValueError(f"Failed to delete dataset after 3 attempts: {str(e)}")
            print(f"Retrying delete for {dataset_id_or_name}... (Attempt {attempt + 1})")
            time.sleep(2)  # Wait before retrying

def get_all_organizations():
    # Get all organizations including deleted ones
    endpoint = urljoin(CKAN_URL, "api/3/action/organization_list")
    
    try:
        response = requests.post(
            endpoint,
            headers={"Authorization": API_KEY},
            json={"all_fields": True, "include_deleted": True},
            timeout=30
        )
        response.raise_for_status()
        return response.json().get("result", [])
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching organizations: {str(e)}")
        return []

def purge_organization(name):
    # Completely remove an organization
    endpoint = urljoin(CKAN_URL, "api/3/action/organization_purge")
    
    try:
        response = requests.post(
            endpoint,
            headers={"Authorization": API_KEY},
            json={"id": name},
            timeout=30
        )
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Error purging organization {name}: {str(e)}")
        return {"success": False, "error": str(e)}

def purge_all_organizations():
    # Main function to purge/destroy/kill all organizations and their datasets
    orgs = get_all_organizations()
    if not orgs:
        print("No organizations found to purge.")
        return []
    
    print(f"Found {len(orgs)} organizations to purge")
    results = []
    
    for org in orgs:
        org_name = org.get("name")
        if not org_name:
            continue
            
        print(f"\nProcessing organization: {org_name}")
        
        # Step 1: Delete all datasets
        datasets = list_all_datasets(CKAN_URL, org_name, API_KEY)
        print(f"Found {len(datasets)} datasets to delete")
        
        for dataset in datasets:
            try:
                delete_dataset(CKAN_URL, dataset, API_KEY)
            except Exception as e:
                print(f"Failed to delete dataset {dataset}: {str(e)}")
                continue
        
        # Step 2: Purge the organization
        print(f"Purging organization {org_name}...")
        try:
            result = purge_organization(org_name)
            status = {
                "name": org_name,
                "success": result.get("success", False),
                "error": result.get("error")
            }
            results.append(status)
            print(f"Purge status: {'Success' if status['success'] else 'Failed'}")
        except Exception as e:
            print(f"Critical error purging {org_name}: {str(e)}")
            results.append({
                "name": org_name,
                "success": False,
                "error": str(e)
            })
    
    # Print summary
    print("\nPurge Summary:")
    success_count = sum(1 for r in results if r.get("success!"))
    print(f"Successfully purged {success_count}/{len(results)} organizations")
    
    if len(results) != success_count:
        print("\nFailed organizations:")
        for org in [r for r in results if not r.get("success!")]:
            print(f"- {org['name']}: {org.get('error', 'Unknown error')}")
    
    return results

if __name__ == "__main__":
    purge_all_organizations()