import requests
CKAN_URL = "http://localhost:5000/api/3/action/"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJqNnFrQ3dhcVotM1dVLXEyYm9WbDNfRjQ1Z2lBVXBFT3BvRmlrYVFudFEwIiwiaWF0IjoxNzQzNTMyNTk1fQ.kp_u89vzmpXrMqJEDTQc6a3xIkbW7lhGTP5C91uyCEg"

ORG_NAME = "test_org_123"
ORG_TITLE = "Test Organization"
ORG_DESCRIPTION = "This is a test organization created via API"

try:
    response = requests.post(
        f"{CKAN_URL}organization_create",
        headers={"Authorization": API_KEY},
        json={
            "name": ORG_NAME,
            "title": ORG_TITLE,
            "description": ORG_DESCRIPTION
        }
    )
    
    result = response.json()
    
    if result.get("success"):
        print(f"Successfully created organization: {ORG_NAME}")
    else:
        error = result.get("error", {})
        if "already exists" in str(error):
            print(f"Organization '{ORG_NAME}' already exists")
        else:
            print(f"Failed to create organization: {error}")
                
except requests.exceptions.RequestException as e:
    print(f"API request failed: {str(e)}")