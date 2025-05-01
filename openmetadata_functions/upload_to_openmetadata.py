import requests
import base64
from urllib.parse import quote
import time

DATA_GOV_API = "https://catalog.data.gov/api/3"
OPENMETADATA_URL = "http://localhost:8585/api/v1"

# API Endpoints
LOGIN_URL = f"{OPENMETADATA_URL}/users/login"
TEAMS_URL = f"{OPENMETADATA_URL}/teams"
SERVICES_URL = f"{OPENMETADATA_URL}/services/databaseServices"
DATABASES_URL = f"{OPENMETADATA_URL}/databases"
SCHEMAS_URL = f"{OPENMETADATA_URL}/databaseSchemas"
TABLES_URL = f"{OPENMETADATA_URL}/tables"

def clean_name(name, max_length=60):
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
    """Set up required services and databases"""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    # 1. Datalake Service
    service_data = {
        "name": "service",
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
                "bucketName": "organizations"
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
            json={"name": "organizations", "service": "service"}
        )
        print("Created database")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code != 409:
            raise
        print("Database already exists")

def create_property_definition(property_name, property_type, description):
    headers = {
        "Authorization": f"Bearer {get_auth_token()}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "name": property_name,
        "description": description,
        "propertyType": {
            "id": property_type.lower(),
            "type": "type",
            "name": property_type
        },
        "entityType": "databaseSchema"
    }
    
    try:
        response = requests.post(
            f"{OPENMETADATA_URL}/metadata/schema/fields",
            headers=headers,
            json=payload
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:  # 409 means already exists
            raise

def create_and_apply_schema_property(schema_fqn, property_name, property_value, property_type="STRING", description=""):
    # First create the property definition (if it doesn't exist)
    create_property_definition(property_name, property_type, description)
    
    # Then apply to specific schema
    headers = {
        "Authorization": f"Bearer {get_auth_token()}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "extension": {
            property_name: property_value
        }
    }
    
    response = requests.patch(
        f"{OPENMETADATA_URL}/databaseSchemas/name/{schema_fqn}",
        headers=headers,
        json=payload
    )
    response.raise_for_status()
    return response.json()

def get_or_create_schema(token, organization):
    """Get or create a schema based on the organization"""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    if not organization:
        # If no organization, use a default schema
        org_name = "no_organization"
    else:
        org_name = clean_name(organization.get("name", "no_organization"))
    
    schema_fqn = f"service.organizations.{org_name}"
    
    try:
        # Check if schema exists
        response = requests.get(
            f"{SCHEMAS_URL}/name/{schema_fqn}",
            headers=headers
        )
        if response.status_code == 200:
            return org_name
        
        # Create new schema
        schema_data = {
            "name": org_name,
            "database": "service.organizations",
            "description": organization.get("description", f"Organization {org_name}")
        }
        response = requests.post(SCHEMAS_URL, headers=headers, json=schema_data)
        response.raise_for_status()
        return org_name
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:  # Already exists
            return org_name
        print(f"Failed to create schema: {str(e)}")
        raise

def get_dataset_metadata(dataset_id):
    """Get dataset metadata from CKAN"""
    url = f"{DATA_GOV_API}/action/package_show?id={quote(dataset_id)}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()["result"]

def create_tag_if_not_exists(token, tag, category="Classification", default_description="No description provided"):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    tag_name_raw = tag.get("display_name") or tag.get("name")
    if not tag_name_raw:
        return None  # Skip if no tag name

    tag_name = clean_name(tag_name_raw).lower()

    # Check if classification exists
    class_check = requests.get(f"{OPENMETADATA_URL}/classifications/name/{category}", headers=headers)
    if class_check.status_code == 404:
        # Create classification if it doesn't exist
        class_payload = {
            "name": category,
            "description": f"{category} autogenerated by script",
            "provider": "user",
            "mutuallyExclusive": False
        }
        create_class = requests.post(f"{OPENMETADATA_URL}/classifications", headers=headers, json=class_payload)
        create_class.raise_for_status()
    elif class_check.status_code >= 400:
        print(f"Error fetching classification: {class_check.status_code}")
        class_check.raise_for_status()

    # Check if tag exists
    tag_check_url = f"{OPENMETADATA_URL}/tags/{tag_name}"
    tag_check_response = requests.get(tag_check_url, headers=headers)
    if tag_check_response.status_code == 404 or tag_check_response.status_code == 500:
        try:
            tag_payload = {
                "name": tag_name,
                "description": tag.get("description", default_description),
                "classification": "Classification"
            }
            tag_create_response = requests.post(
                f"{OPENMETADATA_URL}/tags", headers=headers, json=tag_payload
            )
            tag_create_response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 409:
                print(f"Error creating tag: {e.response.text}")
                raise
                
    return f"{category}.{tag_name}"

def create_resource_columns(token, resources):
    """Create columns for each resource while preserving original names"""
    columns = []
    for resource in resources:
        col_name = f"{clean_name(resource.get('name'))}_{resource.get('format')}"
        format_raw = resource.get('format')
        tags = []
        if format_raw:
            tag_fqn = create_tag_if_not_exists(
                token, {"name": format_raw}, category="Classification"
            )
            if tag_fqn:
                tags.append({"tagFQN": tag_fqn})

        columns.append({
            "name": col_name,
            "dataType": "STRING",
            "description": (
                f"Format: {format_raw or 'unknown'}\n"
                f"URL: {resource['url']}"
            ),
            "dataTypeDisplay": "URL",
            "tags": tags or None
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
    
    schema_name = get_or_create_schema(token, dataset_meta.get("organization"))
    
    #org_name = clean_name(dataset_meta.get("organization").get("name", "no_organization"))       trying to keep the image_url, for some reason didnt work
    #schema_fqn = f"service.organizations.{org_name}"                                              
    #create_and_apply_schema_property(schema_fqn, "image_url", dataset_meta.get("organization").get("image_url"),"STRING", "image url of the organization")


    table_name = clean_name(dataset_meta["name"])
    display_name = dataset_meta['title'][:128]

    # Get the dataset description and append organization details to it
    description = dataset_meta.get("notes", "No description provided")
    
    # Prepare tags
    tags = []
    for tag in dataset_meta.get("tags", []):
        tag_fqn = create_tag_if_not_exists(token, tag, category="Classification")
        if tag_fqn:
            tags.append({"tagFQN": tag_fqn})

    # Create table with all resources as columns
    table_data = {
        "name": table_name,
        "displayName": display_name,
        "description": description,
        "tableType": "External",
        "columns": create_resource_columns(token, dataset_meta.get("resources", [])),
        "databaseSchema": f"service.organizations.{schema_name}",
        "owners": [{"id": team_id, "type": "team"}],
        "tags": tags if tags else None
    }

    try:
        response = requests.post(TABLES_URL, headers=headers, json=table_data)
        response.raise_for_status()
        print(f"Created table for dataset: {display_name} in schema: {schema_name}")
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