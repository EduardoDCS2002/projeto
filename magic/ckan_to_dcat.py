def ckan_to_dcat(dataset: dict) -> dict:
    """
    Converte um dataset do CKAN para um objeto JSON-LD baseado em DCAT,
    com todos os campos incluídos como extensões quando necessário.
    """
    dcat = {
        "@context": "https://www.w3.org/ns/dcat.jsonld",
        "@type": "dcat:Dataset",
        "dct:title": dataset.get("title"),
        "dct:description": dataset.get("notes"),
        "dct:identifier": dataset.get("name"),
        "dct:publisher": {
            "@type": "foaf:Agent",
            "foaf:name": dataset.get("organization", {}).get("title")
        } if dataset.get("organization") else None,
        "dcat:keyword": [tag["name"] for tag in dataset.get("tags", [])],
        "dct:issued": dataset.get("metadata_created"),
        "dct:modified": dataset.get("metadata_modified"),
        "dcat:distribution": [],
        "ckan:resources": dataset.get("resources", []),
        "ckan:extras": dataset.get("extras", []),
        "ckan:owner_org": dataset.get("owner_org"),
        "ckan:author": dataset.get("author"),
        "ckan:author_email": dataset.get("author_email"),
        "ckan:maintainer": dataset.get("maintainer"),
        "ckan:maintainer_email": dataset.get("maintainer_email"),
        "ckan:license_id": dataset.get("license_id"),
        "ckan:license_title": dataset.get("license_title"),
        "ckan:license_url": dataset.get("license_url"),
        "ckan:isopen": dataset.get("isopen"),
        "ckan:state": dataset.get("state"),
        "ckan:groups": dataset.get("groups"),
        "ckan:relationships_as_object": dataset.get("relationships_as_object", []),
        "ckan:relationships_as_subject": dataset.get("relationships_as_subject", [])
    }

    # Converte resources para distribuições padrão DCAT
    for res in dataset.get("resources", []):
        dist = {
            "@type": "dcat:Distribution",
            "dct:title": res.get("name"),
            "dcat:mediaType": res.get("mimetype"),
            "dcat:accessURL": {"@id": res.get("url")},
            "dct:description": res.get("description"),
            "dct:issued": res.get("created"),
            "dct:modified": res.get("last_modified")
        }
        dcat["dcat:distribution"].append(dist)

    # Remover campos None
    return {k: v for k, v in dcat.items() if v is not None}
