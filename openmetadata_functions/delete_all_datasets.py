import requests
from typing import List
import base64

def get_auth_token(openmetadataurl: str) -> str:
    headers = {"Content-Type": "application/json"}
    encoded_password = base64.b64encode("admin".encode('utf-8')).decode('utf-8')
    payload = {
        "email": "admin@open-metadata.org",
        "password": encoded_password
    }
    response = requests.post(f"{openmetadataurl}/users/login", headers=headers, json=payload)
    response.raise_for_status()
    return response.json()["accessToken"]

def get_datasets(api_url: str, headers: dict, limit: int = 1000) -> List[dict]:
    """Get datasets with a single request"""
    try:
        response = requests.get(
            f"{api_url}/tables?limit={limit}",
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        return response.json().get("data", [])
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch datasets: {str(e)}")
        return []

def delete_datasets(api_url: str, headers: dict, datasets: List[dict]):
    """Delete datasets without any delays"""
    success_count = 0
    for i, dataset in enumerate(datasets, 1):
        dataset_id = dataset.get("id")
        fqn = dataset.get("fullyQualifiedName", "unknown")
        
        if not dataset_id:
            print(f"Skipping dataset with missing ID: {fqn}")
            continue
            
        try:
            response = requests.delete(
                f"{api_url}/tables/{dataset_id}?hardDelete=true",
                headers=headers,
                timeout=10
            )
            response.raise_for_status()
            success_count += 1
            print(f"Deleted ({i}/{len(datasets)}): {fqn}")
        except Exception as e:
            print(f"Failed to delete {fqn}: {str(e)}")
    
    print(f"\nDeletion summary: {success_count} succeeded, {len(datasets)-success_count} failed")

if __name__ == "__main__":
    # Configuration
    BASE_URL = "http://localhost:8585/api/v1"
    TOKEN = get_auth_token(BASE_URL)
    
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }

    # Fetch and delete
    print("Fetching datasets...")
    datasets = get_datasets(BASE_URL, headers)
    
    if not datasets:
        print("No datasets found")
        exit()
    
    print(f"Found {len(datasets)} datasets to delete")
    confirm = input("Write anything to confirm deletion: ")
    if confirm != "":
        delete_datasets(BASE_URL, headers, datasets)