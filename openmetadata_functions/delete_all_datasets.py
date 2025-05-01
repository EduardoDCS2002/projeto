import requests
import base64
from typing import List, Dict

def get_auth_token(openmetadata_url: str) -> str:
    """Get authentication token for OpenMetadata"""
    headers = {"Content-Type": "application/json"}
    encoded_password = base64.b64encode("admin".encode('utf-8')).decode('utf-8')
    payload = {
        "email": "admin@open-metadata.org",
        "password": encoded_password
    }
    response = requests.post(f"{openmetadata_url}/users/login", headers=headers, json=payload)
    response.raise_for_status()
    return response.json()["accessToken"]

def get_all_schemas(api_url: str, headers: dict) -> List[Dict]:
    """Get all database schemas"""
    try:
        response = requests.get(
            f"{api_url}/databaseSchemas?limit=1000",
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        return response.json().get("data", [])
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch schemas: {str(e)}")
        return []

def get_tables_in_schema(api_url: str, headers: dict, schema_fqn: str) -> List[Dict]:
    """Get all tables in a specific schema"""
    try:
        response = requests.get(
            f"{api_url}/tables?databaseSchema={schema_fqn}&limit=1000",
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        return response.json().get("data", [])
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch tables for schema {schema_fqn}: {str(e)}")
        return []

def delete_table(api_url: str, headers: dict, table_id: str, table_fqn: str) -> bool:
    """Delete a single table"""
    try:
        response = requests.delete(
            f"{api_url}/tables/{table_id}?hardDelete=true",
            headers=headers,
            timeout=10
        )
        response.raise_for_status()
        print(f"Deleted table: {table_fqn}")
        return True
    except Exception as e:
        print(f"Failed to delete table {table_fqn}: {str(e)}")
        return False

def delete_schema(api_url: str, headers: dict, schema_id: str, schema_fqn: str) -> bool:
    """Delete a single schema"""
    try:
        response = requests.delete(
            f"{api_url}/databaseSchemas/{schema_id}?hardDelete=true",
            headers=headers,
            timeout=10
        )
        response.raise_for_status()
        print(f"Deleted schema: {schema_fqn}")
        return True
    except Exception as e:
        print(f"Failed to delete schema {schema_fqn}: {str(e)}")
        return False

def delete_all_data(api_url: str, headers: dict):
    """Main deletion workflow"""
    # Phase 1: Get all schemas
    print("Fetching all schemas...")
    schemas = get_all_schemas(api_url, headers)
    
    if not schemas:
        print("No schemas found")
        return
    
    print(f"Found {len(schemas)} schemas")
    
    # Phase 2: Process each schema
    total_tables_deleted = 0
    total_schemas_deleted = 0
    
    for schema in schemas:
        schema_fqn = schema.get("fullyQualifiedName", "unknown")
        schema_id = schema.get("id")
        
        if not schema_id:
            print(f"Skipping schema with missing ID: {schema_fqn}")
            continue
        
        # Skip system schemas (optional)
        if schema_fqn.lower().startswith(("system.", "information_schema.")):
            print(f"Skipping system schema: {schema_fqn}")
            continue
        
        # Get all tables in this schema
        print(f"\nProcessing schema: {schema_fqn}")
        tables = get_tables_in_schema(api_url, headers, schema_fqn)
        
        # Delete all tables in schema
        tables_deleted = 0
        for table in tables:
            table_id = table.get("id")
            table_fqn = table.get("fullyQualifiedName", "unknown")
            
            if not table_id:
                print(f"Skipping table with missing ID: {table_fqn}")
                continue
            
            if delete_table(api_url, headers, table_id, table_fqn):
                tables_deleted += 1
        
        # Delete schema after all its tables are deleted (or if it was empty)
        if delete_schema(api_url, headers, schema_id, schema_fqn):
            total_schemas_deleted += 1
        
        total_tables_deleted += tables_deleted
        print(f"Schema summary: Deleted {tables_deleted} tables")
    
    # Final summary
    print("\nDeletion completed:")
    print(f"Total tables deleted: {total_tables_deleted}")
    print(f"Total schemas deleted: {total_schemas_deleted}")

if __name__ == "__main__":
    # Configuration
    BASE_URL = "http://localhost:8585/api/v1"
    TOKEN = get_auth_token(BASE_URL)
    
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }

    # Confirm before deletion
    confirm = input("Type anything to confirm: ")
    if confirm != "":
        delete_all_data(BASE_URL, headers)
    else:
        print("Deletion cancelled")