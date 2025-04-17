import requests
from urllib.parse import quote
import base64

DATA_GOV_API = "https://catalog.data.gov/api/3"
OPENMETADATA_URL = "http://localhost:8585/api/v1"
LOGIN_URL = f"{OPENMETADATA_URL}/users/login"
SERVICES_URL = f"{OPENMETADATA_URL}/services/databaseServices"
DATABASES_URL = f"{OPENMETADATA_URL}/databases"
SCHEMAS_URL = f"{OPENMETADATA_URL}/databaseSchemas"
TABLES_URL = f"{OPENMETADATA_URL}/tables"
"""
# Valid DatabaseServiceTypes from OpenMetadata source code
VALID_SERVICE_TYPES = [
    "BigQuery",
    "Mysql",
    "Redshift",
    "Snowflake",
    "Postgres",
    "Mssql",
    "Hive",
    "Oracle",
    "DeltaLake",
    "Datalake",
    "Db2",
    "Druid",
    "Vertica",
    "Glue",
    "Athena"
]
"""

def get_auth_token():
    """Authentication"""
    
    headers = {
        "Content-Type": "application/json"
    }
    encoded_password = base64.b64encode("admin".encode('utf-8')).decode('utf-8')
    payload = { # assuming you are only using the admin account for openmetadata
        "email": "admin@open-metadata.org",
        "password": encoded_password
    }
    
    response = requests.post(LOGIN_URL, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()["accessToken"]

def get_dataset_metadata(dataset_id):
    """Get complete dataset metadata from data.gov"""
    url = f"{DATA_GOV_API}/action/package_show?id={quote(dataset_id)}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()["result"]


def create_entity(token, url, data):
    """Helper to create any entity"""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.post(url, headers=headers, json=data)
    try:
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 409:  # Already exists
            return {"status": "already_exists"}
        print(f"Error creating entity: {response.text}")
        raise

def setup_infrastructure(token):
    """Create required service, database, and schema"""
    # Create Database Service
    service_data = {
        "name": "data_gov_service",
        "serviceType": "Datalake",  # Using a valid service type
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
    create_entity(token, SERVICES_URL, service_data)

    # Create Database
    db_data = {
        "name": "external_datasets",
        "service": "data_gov_service"
    }
    create_entity(token, DATABASES_URL, db_data)

    # Create Schema
    schema_data = {
        "name": "data_gov",
        "database": "data_gov_service.external_datasets"
    }
    create_entity(token, SCHEMAS_URL, schema_data)

def create_table(token, dataset_meta, resource):
    """Create a table in OpenMetadata"""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    table_name = f"{dataset_meta['name'].lower().replace('-', '_')}_{resource['format'].lower()}" # each table for each dataset is named after
                                                                                                  # the dataset together with the resource
    table_data = {
        "name": table_name[:64],
        "displayName": f"{dataset_meta['title'][:64]} - {resource.get('name', 'Data')[:32]}",
        "description": resource.get('description', dataset_meta.get('notes', ''))[:500],
        "tableType": "External",
        "sourceUrl": resource["url"],
        "columns": [{
            "name": "data_reference",
            "dataType": "STRING",
            "description": "Reference to external data resource"
        }],
        "databaseSchema": "data_gov_service.external_datasets.data_gov"
    }
    
    response = requests.post(TABLES_URL, headers=headers, json=table_data)
    response.raise_for_status()
    return response.json()

def main(dataset_id):
    try:
        print("Authenticating...")
        token = get_auth_token()
        
        print(f"Fetching {dataset_id}...")
        dataset_meta = get_dataset_metadata(dataset_id)
        
        results = []
        for resource in dataset_meta.get("resources", []):
            if not resource.get("url"):
                continue
                
            print(f"Processing: {resource.get('name', resource['id'])}")
            try:
                result = create_table(token, dataset_meta, resource)
                results.append({
                    "table": result["name"],
                    "url": resource["url"]
                })
            except Exception as e:
                print(f"Failed to create table: {str(e)}")
                continue
        
        print(f"\nCreated {len(results)} tables:")
        for res in results:
            print(f"- {res['table']} ({res['url']})")
            
    except Exception as e:
        print(f"Critical error: {str(e)}")

if __name__ == "__main__":
    main("electric-vehicle-population-data")