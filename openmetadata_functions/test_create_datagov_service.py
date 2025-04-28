import requests
import base64

# Create the service
encoded_password = base64.b64encode("admin".encode('utf-8')).decode('utf-8')
payload = { 
    "email": "admin@open-metadata.org",
    "password": encoded_password
}
headers = {
    "Content-Type": "application/json"
}
AUTH_TOKEN = requests.post("http://localhost:8585/api/v1/users/login", headers=headers, json=payload).json()["accessToken"]
headers = {
        "Authorization": f"Bearer {AUTH_TOKEN}",
        "Content-Type": "application/json"
}
OPENMETADATA_URL = "http://localhost:8585/api/v1"

service_entity = {
    "name": "data_gov_connector",
    "serviceType": "external",
    "description": "Connection to data.gov datasets"
}
requests.post(f"{OPENMETADATA_URL}/services/databaseServices", 
             headers=headers, 
             json=service_entity)

# Create database
database_entity = {
    "name": "external_datasets",
    "service": "data_gov_connector"
}
requests.post(f"{OPENMETADATA_URL}/databases", 
             headers=headers, 
             json=database_entity)

# Create schema
schema_entity = {
    "name": "data_gov",
    "database": "data_gov_connector.external_datasets"
}
requests.post(f"{OPENMETADATA_URL}/databaseSchemas", 
             headers=headers, 
             json=schema_entity)