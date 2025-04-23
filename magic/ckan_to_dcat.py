def ckan_to_dcat(dataset: dict) -> dict:
    """
    Converte um dataset do CKAN para um objeto JSON-LD baseado no DCAT.
    Inclui extensões para manter todos os campos relevantes.
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
        "ckan:extras": dataset.get("extras", []),  # Campos adicionais
        "ckan:resources": dataset.get("resources", []),  # Manter original
        "ckan:owner_org": dataset.get("owner_org"),
        "ckan:author": dataset.get("author"),
        "ckan:maintainer": dataset.get("maintainer"),
    }

    # Distribuições (recursos)
    for res in dataset.get("resources", []):
        dist = {
            "@type": "dcat:Distribution",
            "dct:title": res.get("name"),
            "dcat:mediaType": res.get("mimetype"),
            "dcat:accessURL": {"@id": res.get("url")}
        }
        dcat["dcat:distribution"].append(dist)

    # Remover campos None
    dcat = {k: v for k, v in dcat.items() if v is not None}

    return dcat
