import requests

def dcat_to_ckan(dcat_dataset: dict, ckan_url: str, api_key: str) -> dict:
    """
    Cria (ou atualiza) um dataset no CKAN a partir de um objeto DCAT JSON-LD.
    """
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json"
    }

    # Conversão básica de DCAT para CKAN
    dataset = {
        "name": dcat_dataset["dct:identifier"],
        "title": dcat_dataset.get("dct:title"),
        "notes": dcat_dataset.get("dct:description"),
        "tags": [{"name": tag} for tag in dcat_dataset.get("dcat:keyword", [])],
        "owner_org": dcat_dataset.get("ckan:owner_org"),
        "author": dcat_dataset.get("ckan:author"),
        "maintainer": dcat_dataset.get("ckan:maintainer"),
        "extras": dcat_dataset.get("ckan:extras", []),
        "resources": []
    }

    # Recursos (distributions)
    for dist in dcat_dataset.get("dcat:distribution", []):
        resource = {
            "name": dist.get("dct:title", "resource"),
            "url": dist.get("dcat:accessURL", {}).get("@id", ""),
            "mimetype": dist.get("dcat:mediaType")
        }
        dataset["resources"].append(resource)

    # Envia ao CKAN
    response = requests.post(
        f"{ckan_url}package_create", 
        headers=headers, 
        json=dataset
    )

    if response.status_code == 409:
        # Já existe, tenta atualizar
        print("Dataset already exists, attempting to update...")
        response = requests.post(
            f"{ckan_url}package_update",
            headers=headers,
            json=dataset
        )

    response.raise_for_status()
    return response.json()["result"]
