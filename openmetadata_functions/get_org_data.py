import requests
import json
import base64
from urllib.parse import quote

OPENMETADATA_URL = "http://localhost:8585/api/v1"
TEAMS_URL = f"{OPENMETADATA_URL}/teams"
TABLES_URL = f"{OPENMETADATA_URL}/tables"

def get_auth_token():
    """Get OpenMetadata auth token"""
    headers = {"Content-Type": "application/json"}
    response = requests.post(
        f"{OPENMETADATA_URL}/users/login",
        headers=headers,
        json={
            "email": "admin@open-metadata.org",
            "password": base64.b64encode("admin".encode()).decode()
        }
    )
    return response.json()["accessToken"]

def get_ckan_org_from_team(token: str, team_name: str) -> dict:
    """
    Extract CKAN organization data from a team's description field.
    Returns the full CKAN org dictionary.
    """
    headers = {"Authorization": f"Bearer {token}"}
    
    # Fetch team
    response = requests.get(
        f"{TEAMS_URL}/name/{quote(team_name)}",
        headers=headers
    )
    response.raise_for_status()
    team = response.json()
    
    # Parse description to extract JSON
    description = team.get("description", "")
    if "Full Metadata:" not in description:
        raise ValueError("No CKAN metadata found in team description")
    
    # Extract and parse JSON
    json_str = description.split("Full Metadata:\n")[1].strip()
    return json.loads(json_str)

def get_ckan_org_from_table(token: str, table_fqn: str) -> dict:
    """
    Get CKAN organization data from a table's extension field.
    Example table_fqn: 'data_gov_service.external_datasets.data_gov.electric_vehicles_1234'
    """
    headers = {"Authorization": f"Bearer {token}"}
    
    # Fetch table
    response = requests.get(
        f"{TABLES_URL}/name/{quote(table_fqn)}",
        headers=headers
    )
    response.raise_for_status()
    table = response.json()
    
    # Access nested extension field
    return table.get("extension", {}).get("ckan", {}).get("organization", {})

# Example usage
if __name__ == "__main__":
    token = get_auth_token()
    
    # Test with known team and table
    try:
        # 1. Fetch from team
        team_name = "org_wa_gov"  # Example team name
        org_from_team = get_ckan_org_from_team(token, team_name)
        print(f"From Team '{team_name}':")
        print(json.dumps(org_from_team, indent=2))
        
        # 2. Fetch from table
        table_fqn = "data_gov_service.external_datasets.data_gov.electric_vehicles_1234"  # Example
        org_from_table = get_ckan_org_from_table(token, table_fqn)
        print(f"\nFrom Table '{table_fqn}':")
        print(json.dumps(org_from_table, indent=2))
        
        # Verify data matches
        assert org_from_team["id"] == org_from_table["id"], "Organization IDs don't match!"
        print("\nBoth sources return the same organization ID")
        
    except Exception as e:
        print(f"Error: {str(e)}")