import requests
import base64

OPENMETADATA_URL = "http://localhost:8585/api/v1"

# API Endpoints
LOGIN_URL = f"{OPENMETADATA_URL}/users/login"
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

def get_all_schemas(token):
    """Get all schemas from the organizations database"""
    headers = {"Authorization": f"Bearer {token}"}
    
    # Get all schemas under the 'service.organizations' database
    params = {
        "database": "service.organizations",
        "limit": 1000  # Adjust if you have more than 1000 schemas
    }
    
    response = requests.get(SCHEMAS_URL, headers=headers, params=params)
    response.raise_for_status()
    
    return response.json().get("data", [])

def get_tables_for_schema(token, schema_fqn):
    """Get all tables for a specific schema"""
    headers = {"Authorization": f"Bearer {token}"}
    
    params = {
        "databaseSchema": schema_fqn,
        "limit": 1000  # Adjust if you have more than 1000 tables per schema
    }
    
    response = requests.get(TABLES_URL, headers=headers, params=params)
    response.raise_for_status()
    
    return response.json().get("data", [])

def get_schema_tables_dict():
    """Main function that returns a dictionary of schemas with their tables"""
    try:
        # Authenticate
        token = get_auth_token()
        
        # Get all schemas
        schemas = get_all_schemas(token)
        
        # Create dictionary to store the result
        schema_tables_dict = {}
        
        # Get tables for each schema
        for schema in schemas:
            schema_fqn = schema["fullyQualifiedName"]
            tables = get_tables_for_schema(token, schema_fqn)
            
            # Store tables in the dictionary with schema name as key
            schema_tables_dict[schema_fqn] = tables
        
        return tables
        
    except Exception as e:
        print(f"\nError: {str(e)}")
        return None

if __name__ == "__main__":
    result = get_schema_tables_dict()
    
    if result:
        # Print the results
        print("Schema and Tables Dictionary:")
        print("=" * 50)
        for table in result:
            print(table)
    else:
        print("Failed to retrieve schema and table information.")