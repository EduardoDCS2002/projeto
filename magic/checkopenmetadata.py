import requests
import json
import base64
from typing import List, Dict, Optional

OPENMETADATA_URL = "http://localhost:8585/api/v1"
LOGIN_URL = f"{OPENMETADATA_URL}/users/login"

def get_auth_token() -> str:
    """Authenticate and return access token"""
    headers = {"Content-Type": "application/json"}
    encoded_password = base64.b64encode("admin".encode('utf-8')).decode('utf-8')
    payload = {
        "email": "admin@open-metadata.org",
        "password": encoded_password
    }
    response = requests.post(LOGIN_URL, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()["accessToken"]

TOKEN = get_auth_token()

def search_tables(
    token: str,
    query: str = "*",
    service_name: Optional[str] = None,
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    limit: int = 50
) -> List[Dict]:
    """Search for tables in OpenMetadata with optional filters"""
    endpoint = f"{OPENMETADATA_URL}/search/query"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Build query string with filters
    query_parts = [query]
    if service_name:
        query_parts.append(f"service.name:{service_name}")
    if database_name:
        query_parts.append(f"database.name:{database_name}")
    if schema_name:
        query_parts.append(f"databaseSchema.name:{schema_name}")
    
    params = {
        "q": " AND ".join(query_parts),
        "entityType": "table",
        "from": 0,
        "size": limit
    }

    try:
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()
        return response.json().get("hits", {}).get("hits", [])
    except requests.exceptions.HTTPError as e:
        print(f"Search error: {e.response.text}")
        return []

def get_table_details(token: str, fqn: str) -> Optional[Dict]:
    """Get complete details for a specific table"""
    endpoint = f"{OPENMETADATA_URL}/tables/name/{fqn}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(endpoint, headers=headers, params={"fields": "*"})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"Error fetching table {fqn}: {e.response.text}")
        return None

def print_table_summary(table: Dict) -> None:
    """Print formatted table summary"""
    source = table.get("_source", {})
    
    print(f"\n{'='*50}")
    print(f"Table: {source.get('name')}")
    print(f"FQN: {source.get('fullyQualifiedName')}")
    print(f"Type: {source.get('tableType', 'Unknown')}")
    
    if source.get('description'):
        print(f"\nDescription: {source['description'][:200]}...")
    
    if source.get('tags'):
        print("\nTags:")
        for tag in source['tags']:
            print(f" - {tag.get('tagFQN')}")
    
    if source.get('columns'):
        print("\nColumns:")
        for col in source['columns']:
            print(f" - {col.get('name')} ({col.get('dataType')}): {col.get('description', 'No description')[:50]}")

def print_table_details(table: Dict) -> None:
    """Print complete table details"""
    print("\nComplete Metadata:")
    print(json.dumps(table, indent=2))

def main():  # fancy way to search for datasets
    # Get search parameters from user
    search_query = input("Enter search query (leave blank for all tables): ") or "*"
    service_filter = input("Filter by service name (leave blank for all): ") or None
    database_filter = input("Filter by database name (leave blank for all): ") or None
    schema_filter = input("Filter by schema name (leave blank for all): ") or None
    
    # Search for tables
    tables = search_tables(
        TOKEN,
        query=search_query,
        service_name=service_filter,
        database_name=database_filter,
        schema_name=schema_filter
    )
    
    if not tables:
        print("\nNo tables found matching your criteria")
        return
    
    print(f"\nFound {len(tables)} tables:")
    for idx, table in enumerate(tables, 1):
        print(f"{idx}. {table['_source']['displayName']}")
    
    # Let user select a table for detailed view
    selection = input("\nEnter table number for details (0 for all, Enter to exit): ")
    if not selection:
        return
    
    if selection == "0":
        for table in tables:
            if details := get_table_details(TOKEN, table['_source']['fullyQualifiedName']):
                print_table_details(details)
    else:
        try:
            idx = int(selection) - 1
            if 0 <= idx < len(tables):
                table = tables[idx]
                if details := get_table_details(TOKEN, table['_source']['fullyQualifiedName']):
                    print_table_details(details)
            else:
                print("Invalid selection")
        except ValueError:
            print("Please enter a valid number")

if __name__ == "__main__":
    main()