import requests
import base64
from urllib.parse import quote
from slugify import slugify

DATA_GOV_API = "https://catalog.data.gov/api/3"
OPENMETADATA_URL = "http://localhost:8585/api/v1"
LOGIN_URL = f"{OPENMETADATA_URL}/users/login"
SERVICES_URL = f"{OPENMETADATA_URL}/services/databaseServices"
DATABASES_URL = f"{OPENMETADATA_URL}/databases"
SCHEMAS_URL = f"{OPENMETADATA_URL}/databaseSchemas"
TABLES_URL = f"{OPENMETADATA_URL}/tables"

def get_auth_token():
    headers = {"Content-Type": "application/json"}
    encoded_password = base64.b64encode("admin".encode('utf-8')).decode('utf-8')
    payload = {
        "email": "admin@open-metadata.org",
        "password": encoded_password
    }
    response = requests.post(LOGIN_URL, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()["accessToken"]

def get_dataset_metadata(dataset_id):
    url = f"{DATA_GOV_API}/action/package_show?id={quote(dataset_id)}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()["result"]

def detect_schema(resource):
    """Generate a simple schema for external references"""
    return [{
        "name": "external_reference",
        "dataType": "STRING",
        "description": f"Reference to {resource['format']} data at {resource['url']}",
        "dataTypeDisplay": "URL"
    }]

def create_table(token, dataset_meta, resource):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    table_name = slugify(
        f"{dataset_meta['name']}_{resource['id'][:8]}",
        separator="_"
    )[:64]

    dcat_metadata = {
        "identifier": dataset_meta.get("id"),
        "issued": dataset_meta.get("metadata_created"),
        "modified": dataset_meta.get("metadata_modified"),
        "publisher": dataset_meta.get("organization", {}).get("title"),
        "contact_point": dataset_meta.get("maintainer"),
        "theme": [g["title"] for g in dataset_meta.get("groups", [])],
        "keyword": [t["display_name"] for t in dataset_meta.get("tags", [])],
        "license": dataset_meta.get("license_title"),
        "spatial": dataset_meta.get("spatial"),
        "temporal": dataset_meta.get("temporal"),
        "accrual_periodicity": dataset_meta.get("accrual_periodicity"),
        "distribution": {
            "format": resource.get("format"),
            "media_type": resource.get("mimetype"),
            "access_url": resource.get("url"),
            "size": resource.get("size")
        }
    }

    table_data = {
        "name": table_name,
        "displayName": f"{dataset_meta['title'][:64]} - {resource.get('name', resource['format'])[:32]}",
        "description": (
            f"{dataset_meta.get('notes', 'No description')}\n\n"
            f"Resource Description: {resource.get('description', '')}"
        )[:2000],
        "tableType": "External",
        "sourceUrl": resource["url"],
        "columns": detect_schema(resource),
        "databaseSchema": "data_gov_service.external_datasets.data_gov",
        "tags": [{"tagFQN": tag["display_name"]} for tag in dataset_meta.get("tags", [])][:10],
        "owners": [{
            "type": "organization",
            "name": dataset_meta.get("organization", {}).get("name", "data_gov"),
            "id": dataset_meta.get("organization", {}).get("id", "")
        }],
        "extension": {
            "dcat": dcat_metadata
        }
    }

    response = requests.post(TABLES_URL, headers=headers, json=table_data)
    response.raise_for_status()
    return response.json()

def setup_infrastructure(token):
    """Create required service, database, and schema"""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    # Create service if not exists
    try:
        service_data = {
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
        requests.post(SERVICES_URL, headers=headers, json=service_data)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code != 409:  # Ignore "already exists" errors
            raise

    # Create database if not exists
    try:
        db_data = {
            "name": "external_datasets",
            "service": "data_gov_service"
        }
        requests.post(DATABASES_URL, headers=headers, json=db_data)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code != 409:
            raise

    # Create schema if not exists
    try:
        schema_data = {
            "name": "data_gov",
            "database": "data_gov_service.external_datasets"
        }
        requests.post(SCHEMAS_URL, headers=headers, json=schema_data)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code != 409:
            raise