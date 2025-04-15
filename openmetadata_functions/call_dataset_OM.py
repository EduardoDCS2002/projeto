import requests
import json

# API do data.gov (CKAN com metadados DCAT)
API_URL = "https://catalog.data.gov/api/3/action/package_search"
params = {
    "q": "climate",  # Termo de busca
    "rows": 1        # Limitar a 1 resultado para exemplo
}

response = requests.get(API_URL, params=params)
data = response.json()

# Extrair o primeiro dataset
dataset = data["result"]["results"][0]
print("Metadados DCAT do dataset:")
print(json.dumps(dataset, indent=2))