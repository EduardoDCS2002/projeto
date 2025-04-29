import requests
import base64
from urllib.parse import quote
from slugify import slugify

DATA_GOV_API = "https://catalog.data.gov/api/3"
OPENMETADATA_URL = "http://localhost:8585/api/v1"

# API Endpoints
LOGIN_URL = f"{OPENMETADATA_URL}/users/login"
TEAMS_URL = f"{OPENMETADATA_URL}/teams"
SERVICES_URL = f"{OPENMETADATA_URL}/services/databaseServices"
DATABASES_URL = f"{OPENMETADATA_URL}/databases"
SCHEMAS_URL = f"{OPENMETADATA_URL}/databaseSchemas"
TABLES_URL = f"{OPENMETADATA_URL}/tables"

def clean_name(name, max_length=64):
    """Minimal cleaning for OpenMetadata names while preserving original as much as possible"""
    if not name:
        return ""
    # Only replace characters that would cause problems in OpenMetadata
    cleaned = name.strip()
    cleaned = cleaned.replace(" ", "_").replace(".", "_").replace("-", "_")
    cleaned = "".join(c for c in cleaned if c.isalnum() or c == "_")
    return cleaned[:max_length]

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

def setup_infrastructure(token):
    """Set up required services, databases, and schemas"""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    # 1. Datalake Service
    service_data = {
        "name": "ckan_service",
        "serviceType": "Datalake",
        "connection": {
            "config": {
                "type": "Datalake",
                "configSource": {
                    "securityConfig": {
                        "awsAccessKeyId": "test",
                        "awsSecretAccessKey": "test",
                        "awsRegion": "us-east-1"
                    }
                },
                "bucketName": "ckan-datasets"
            }
        }
    }
    
    try:
        requests.post(SERVICES_URL, headers=headers, json=service_data)
        print("Created datalake service")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code != 409:  # 409 means already exists
            raise
        print("Datalake service already exists")

    # 2. Database
    try:
        requests.post(
            DATABASES_URL,
            headers=headers,
            json={"name": "ckan_datasets", "service": "ckan_service"}
        )
        print("Created database")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code != 409:
            raise
        print("Database already exists")

    # 3. Schema
    try:
        requests.post(
            SCHEMAS_URL,
            headers=headers,
            json={"name": "ckan", "database": "ckan_service.ckan_datasets"}
        )
        print("Created schema")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code != 409:
            raise
        print("Schema already exists")

def get_dataset_metadata(dataset_id):
    """Get dataset metadata from CKAN"""
    url = f"{DATA_GOV_API}/action/package_show?id={quote(dataset_id)}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()["result"]

def create_resource_columns(resources):
    """Create columns for each resource while preserving original names"""
    columns = []
    for idx, resource in enumerate(resources, 1):
        col_name = clean_name(resource.get('name') or f"resource_{idx}")
        columns.append({
            "name": col_name,
            "dataType": "STRING",
            "description": (
                f"Resource: {resource.get('name', 'Unnamed')}\n"
                f"Format: {resource.get('format', 'unknown')}\n"
                f"URL: {resource['url']}"
            ),
            "dataTypeDisplay": "URL",
            "tags": [
                {"tagFQN": f"ResourceType.{clean_name(resource.get('format', 'data'))}"}
            ] if resource.get('format') else None
        })
    return columns

def get_or_create_team(token, team_name="data_curators"):
    """Get or create a team for ownership"""
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        # Try to get existing team
        response = requests.get(
            f"{OPENMETADATA_URL}/teams/name/{team_name}",
            headers=headers
        )
        if response.status_code == 200:
            return response.json()["id"]

        # Create new team
        response = requests.post(
            TEAMS_URL,
            headers=headers,
            json={
                "name": team_name,
                "displayName": team_name.replace("_", " ").title(),
                "teamType": "Group",
                "description": "Team for managing CKAN datasets"
            }
        )
        response.raise_for_status()
        return response.json()["id"]
    except Exception as e:
        print(f"Failed to get/create team: {str(e)}")
        raise

def create_dataset_table(token, dataset_meta):
    """Create one table per dataset with all resources as columns"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    team_id = get_or_create_team(token)
    # Use original name with minimal cleaning
    table_name = clean_name(dataset_meta['name'])
    display_name = dataset_meta['title'][:128]  # Display names can be longer
    
    # Build comprehensive description
    description = dataset_meta.get("notes", "No description provided")
    if dataset_meta.get("organization"):
        org = dataset_meta["organization"]
        description += f"\n\nPublisher: {org.get('title', 'Unknown')}"
        if org.get("url"):
            description += f" ({org['url']})"
    
    # Prepare tags from CKAN metadata
    tags = []
    if dataset_meta.get("organization"):
        org_tag = clean_name(dataset_meta["organization"].get("name"))
        if org_tag:
            tags.append({"tagFQN": f"Organization.{org_tag}"})
    
    for tag in dataset_meta.get("tags", []):
        tag_name = tag.get("display_name", tag.get("name", "")).strip()
        if tag_name:
            tags.append({"tagFQN": f"CKANTag.{clean_name(tag_name)}"})
    
    # Create table with all resources as columns
    table_data = {
        "name": table_name,
        "displayName": display_name,
        "description": description,
        "tableType": "External",
        "columns": create_resource_columns(dataset_meta.get("resources", [])),
        "databaseSchema": "ckan_service.ckan_datasets.ckan",
        "owners": [{"id": team_id, "type": "team"}],
        "tags": tags if tags else None
    }
    
    try:
        response = requests.post(TABLES_URL, headers=headers, json=table_data)
        response.raise_for_status()
        print(f"Created table for dataset: {display_name}")
        return response.json()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:
            print("Table already exists - consider updating instead")
            raise Exception("Table exists") from e
        print(f"API Error: {e.response.text}")
        raise

def search_datasets(query: str, max_results: int = 10):
    """Search CKAN for datasets"""
    url = f"{DATA_GOV_API}/action/package_search?q={quote(query)}&rows={max_results}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()["result"]["results"]

def process_datasets(token):
    """Main processing function"""
    query = input("Enter search query for datasets: ")
    max_results = int(input("Number of datasets to process: "))
    datasets = search_datasets(query, max_results)
    
    setup_infrastructure(token)
    
    results = {"created": 0, "skipped": 0, "failed": 0}
    
    for dataset in datasets:
        print(f"\nProcessing: {dataset['title']}")
        try:
            create_dataset_table(token, dataset)
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
    try:
        token = get_auth_token()
        process_datasets(token)
    except Exception as e:
        print(f"\nFatal error: {str(e)}")