import requests
import base64
import json
from urllib.parse import quote
from slugify import slugify

DATA_GOV_API = "https://catalog.data.gov/api/3"
OPENMETADATA_URL = "http://localhost:8585/api/v1"
DEFAULT_TEAM_NAME = "Organization"  # Default team in OpenMetadata 1.6.8

# API Endpoints
LOGIN_URL = f"{OPENMETADATA_URL}/users/login"
TEAMS_URL = f"{OPENMETADATA_URL}/teams"
TEAM_BY_NAME_URL = f"{OPENMETADATA_URL}/teams/name"
SERVICES_URL = f"{OPENMETADATA_URL}/services/databaseServices"
DATABASES_URL = f"{OPENMETADATA_URL}/databases"
SCHEMAS_URL = f"{OPENMETADATA_URL}/databaseSchemas"
TABLES_URL = f"{OPENMETADATA_URL}/tables"

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

def get_organization_team_id(token):
    """Get the ID of the Organization team"""
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get(
            f"{OPENMETADATA_URL}/teams/name/Organization",
            headers=headers
        )
        response.raise_for_status()
        return response.json()["id"]
    except Exception as e:
        print(f"❌ Failed to get Organization team ID: {str(e)}")
        raise

def setup_infrastructure(token):
    """Set up required services, databases, and schemas for v1.6.8"""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    # 1. Datalake Service
    try:
        requests.post(
            SERVICES_URL,
            headers=headers,
            json={
                "name": "data_gov_service",
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
                        "bucketName": "test-bucket"
                    }
                }
            }
        )
        print("✅ Created datalake service")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code != 409:
            raise
        print("✓ Datalake service already exists")

    # 2. Database
    try:
        requests.post(
            DATABASES_URL,
            headers=headers,
            json={"name": "external_datasets", "service": "data_gov_service"}
        )
        print("✅ Created database")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code != 409:
            raise
        print("✓ Database already exists")

    # 3. Schema
    try:
        requests.post(
            SCHEMAS_URL,
            headers=headers,
            json={"name": "data_gov", "database": "data_gov_service.external_datasets"}
        )
        print("✅ Created schema")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code != 409:
            raise
        print("✓ Schema already exists")

def get_dataset_metadata(dataset_id):
    """Fetch dataset metadata from CKAN"""
    url = f"{DATA_GOV_API}/action/package_show?id={quote(dataset_id)}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()["result"]

def detect_schema(resource):
    """Generate schema for external tables"""
    return [{
        "name": "external_reference",
        "dataType": "STRING",
        "description": f"Reference to {resource['format']} data at {resource['url']}",
        "dataTypeDisplay": "URL"
    }]

def get_or_create_group_team(token):
    """Get or create a Group-type team for ownership"""
    headers = {"Authorization": f"Bearer {token}"}
    team_name = "data_gov_importers"
    
    try:
        # Try to get existing team
        response = requests.get(
            f"{OPENMETADATA_URL}/teams/name/{team_name}",
            headers=headers
        )
        if response.status_code == 200:
            return response.json()["id"]

        # Create new Group team
        payload = {
            "name": team_name,
            "displayName": "Data.gov Importers",
            "teamType": "Group",
            "description": "Team for imported CKAN datasets"
        }
        
        response = requests.post(
            f"{OPENMETADATA_URL}/teams",
            headers=headers,
            json=payload
        )
        response.raise_for_status()
        return response.json()["id"]
        
    except Exception as e:
        print(f"❌ Failed to get/create Group team: {str(e)}")
        raise

def create_classification_if_not_exists(token, classification_name="Classification"):
    """Ensure the classification exists in OpenMetadata 1.6.8"""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    # First check if it exists
    try:
        response = requests.get(
            f"{OPENMETADATA_URL}/classifications/{classification_name}",
            headers=headers
        )
        if response.status_code == 200:
            return True
    except requests.exceptions.HTTPError:
        pass
    
    # Create if it doesn't exist
    try:
        response = requests.post(
            f"{OPENMETADATA_URL}/classifications",
            headers=headers,
            json={
                "name": classification_name,
                "description": "Default classification for imported tags",
                "categoryType": "CLASSIFICATION"
            }
        )
        response.raise_for_status()
        return True
    except requests.exceptions.HTTPError as e:
        print(f"⚠️ Failed to create classification: {e.response.text}")
        return False


def create_tag_if_not_exists(token, tag_name):
    """Create a tag under the Classification category"""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    classification_name = "Classification"
    
    # First ensure classification exists
    if not create_classification_if_not_exists(token, classification_name):
        return None
    
    # Clean the tag name
    clean_tag = slugify(tag_name, separator="_")
    
    try:
        # Try to create the tag
        response = requests.post(
            f"{OPENMETADATA_URL}/tags",
            headers=headers,
            json={
                "name": clean_tag,
                "description": f"Tag for {clean_tag}",
                "classification": classification_name
            }
        )
        
        if response.status_code in (200, 201, 409):
            return f"{classification_name}.{clean_tag}"
            
        print(f"⚠️ Unexpected response creating tag: {response.text}")
        return None
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:  # Already exists
            return f"{classification_name}.{clean_tag}"
        print(f"⚠️ Failed to create tag: {e.response.text}")
        return None


def create_table(token, team_id, dataset_meta, resource):
    """Create table with Group team ownership - minimal 1.6.8 compatible version"""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    # Generate safe table name
    table_name = slugify(f"{dataset_meta['name']}_{resource['id'][:8]}", separator="_")[:64]
    ckan_org = dataset_meta.get("organization", {})
    
    # Construct basic table payload (without tags or extensions)
    table_data = {
        "name": table_name,
        "displayName": f"{dataset_meta['title'][:64]} - {resource.get('name', resource['format'])[:32]}",
        "description": dataset_meta.get("notes", "No description"),
        "tableType": "External",
        "sourceUrl": resource["url"],
        "columns": detect_schema(resource),
        "databaseSchema": "data_gov_service.external_datasets.data_gov",
        "owners": [{"id": team_id, "type": "team"}]
    }
    
    # Add publisher info to description if available
    if ckan_org.get("title"):
        table_data["description"] += f"\n\nPublisher: {ckan_org['title']}"
        if ckan_org.get("url"):
            table_data["description"] += f" ({ckan_org['url']})"
    
    try:
        response = requests.post(TABLES_URL, headers=headers, json=table_data)
        response.raise_for_status()
        print(f"✅ Created table: {table_name}")
        return response.json()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:
            print("Table already exists - skipping")
            raise Exception("Table already exists") from e
        print(f"API Error: {e.response.text}")
        raise

def search_datasets(query: str, max_results: int = 10):
    """Search CKAN for datasets"""
    url = f"{DATA_GOV_API}/action/package_search?q={quote(query)}&rows={max_results}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()["result"]["results"]

def process_datasets(token, group_team_id):
    """Main processing workflow - simplified for 1.6.8"""
    query = input("Make a query to search datasets: ")
    max_results = int(input("How many datasets do you want? "))
    datasets = search_datasets(query, max_results)
    
    total_created = 0
    total_skipped = 0
    
    for dataset in datasets:
        dataset_id = dataset["id"]
        print(f"\nProcessing dataset: {dataset['title']} (ID: {dataset_id})")
        created = 0
        skipped = 0
        
        try:
            dataset_meta = get_dataset_metadata(dataset_id)
            
            for resource in dataset_meta.get("resources", []):
                if not resource.get("url"):
                    continue
                
                print(f"\nProcessing resource: {resource.get('name', resource['id'])}")
                print(f"Format: {resource.get('format')}")
                print(f"URL: {resource['url']}")
                
                try:
                    result = create_table(token, group_team_id, dataset_meta, resource)
                    created += 1
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 409:
                        skipped += 1
                    else:
                        print(f"Failed to create table: {e.response.text}")
                        skipped += 1
                except Exception as e:
                    print(f"Error creating table: {str(e)}")
                    skipped += 1
            
            print(f"\nSummary for {dataset['title']}:")
            print(f"Created {created} new tables, skipped {skipped} existing tables")
            total_created += created
            total_skipped += skipped
            
        except Exception as e:
            print(f"Error processing dataset {dataset_id}: {str(e)}")
            continue
    
    print(f"\nProcessing complete. Total created: {total_created}, Total skipped: {total_skipped}")

if __name__ == "__main__":
    try:
        token = get_auth_token()
        
        group_team_id = get_or_create_group_team(token)
        
        # 2. Setup infrastructure
        setup_infrastructure(token)
        
        # 3. Process datasets
        process_datasets(token, group_team_id)
        
    except Exception as e:
        print(f"\n🚨 Fatal error: {str(e)}")
        