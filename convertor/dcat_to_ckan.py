import requests

def dcat_to_ckan(dcat_dataset: dict, ckan_url: str, api_key: str) -> dict:
    """
    Cria ou atualiza um dataset CKAN a partir de um DCAT JSON-LD, com fidelidade total.
    """
    headers = {"Authorization": api_key, "Content-Type": "application/json"}

    dataset = {
        "name": dcat_dataset["dct:identifier"],
        "title": dcat_dataset.get("dct:title"),
        "notes": dcat_dataset.get("dct:description"),
        "tags": [{"name": tag} for tag in dcat_dataset.get("dcat:keyword", [])],
        "owner_org": dcat_dataset.get("ckan:owner_org"),
        "author": dcat_dataset.get("ckan:author"),
        "author_email": dcat_dataset.get("ckan:author_email"),
        "maintainer": dcat_dataset.get("ckan:maintainer"),
        "maintainer_email": dcat_dataset.get("ckan:maintainer_email"),
        "license_id": dcat_dataset.get("ckan:license_id"),
        "isopen": dcat_dataset.get("ckan:isopen"),
        "state": dcat_dataset.get("ckan:state"),
        "extras": dcat_dataset.get("ckan:extras", []),
        "groups": dcat_dataset.get("ckan:groups", []),
        "resources": []
    }

    for dist in dcat_dataset.get("dcat:distribution", []):
        resource = {
            "name": dist.get("dct:title", "resource"),
            "url": dist.get("dcat:accessURL", {}).get("@id", ""),
            "mimetype": dist.get("dcat:mediaType"),
            "description": dist.get("dct:description"),
            "created": dist.get("dct:issued"),
            "last_modified": dist.get("dct:modified")
        }
        dataset["resources"].append(resource)

    response = requests.post(f"{ckan_url}package_create", headers=headers, json=dataset)
    
    if response.status_code == 409:
        response = requests.post(f"{ckan_url}package_update", headers=headers, json=dataset)

    response.raise_for_status()
    return response.json()["result"]
