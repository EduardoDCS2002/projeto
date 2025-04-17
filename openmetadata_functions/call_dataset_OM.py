import requests
import json

# API of data.gov (CKAN with metadata DCAT)
API_URL = "https://catalog.data.gov/api/3/action/package_search"
params = {
    "q": "climate",  # Search
    "rows": 1        # Limit (1 result for example)
}

response = requests.get(API_URL, params=params)
data = response.json()

# Extract the first dataset
dataset = data["result"]["results"][0]
print("Metadados DCAT do dataset:")
print(json.dumps(dataset, indent=2))