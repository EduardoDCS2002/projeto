import requests
import base64
DATA_GOV_API = "https://catalog.data.gov/api/3"
OPENMETADATA_URL = "http://localhost:8585/api/v1"
LOGIN_URL = f"{OPENMETADATA_URL}/users/login"
SERVICES_URL = f"{OPENMETADATA_URL}/services/databaseServices"
DATABASES_URL = f"{OPENMETADATA_URL}/databases"
SCHEMAS_URL = f"{OPENMETADATA_URL}/databaseSchemas"
TABLES_URL = f"{OPENMETADATA_URL}/tables"

def get_auth_token():
    # Authentication
    headers = {
        "Content-Type": "application/json"
    }
    encoded_password = base64.b64encode("admin".encode('utf-8')).decode('utf-8')
    payload = { 
        "email": "admin@open-metadata.org",
        "password": encoded_password
    }
    
    response = requests.post(LOGIN_URL, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()["accessToken"]

TOKEN = get_auth_token()

def search_api(
    openmetadata_url: str,
    token: str,
    entity_type: str = "table",
    query: str = "*",
    limit: int = 100
) -> list:
    
    endpoint = f"{openmetadata_url}/search/query"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    params = {
        "q": query,
        "entityType": entity_type,
        "from": 0,
        "size": limit
    }

    try:
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()
        return response.json().get("hits", {}).get("hits", [])
    except requests.exceptions.HTTPError as e:
        print(f"Server Error Details: {e.response.text}")  # Key for debugging!
    return []

def tables_api(
    openmetadata_url: str,
    token: str,
    limit: int = 100
) -> list:

    endpoint = f"{openmetadata_url}/tables"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    params = {"limit": limit}

    try:
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()
        return response.json().get("data", [])
    except Exception as e:
        print(f"Error in tables_api(): {e}")
        return []
    
# Option 1: Search API (recommended)
datasets = search_api(OPENMETADATA_URL, TOKEN, limit=50)
for ds in datasets:
    print(ds["_source"]["name"], "|", ds["_source"]["fullyQualifiedName"])

# Option 2: Tables API (simpler)
tables = tables_api(OPENMETADATA_URL, TOKEN, limit=50)
for table in tables:
    print(table["name"], "|", table["fullyQualifiedName"])