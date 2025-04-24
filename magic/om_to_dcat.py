def openmetadata_to_dcat(table: dict) -> dict:
    """
    Converte uma tabela do OpenMetadata em um DCAT JSON-LD completo.
    Preserva colunas, tags, dono, perfil, uso, etc.
    """
    dcat = {
        "@context": "https://www.w3.org/ns/dcat.jsonld",
        "@type": "dcat:Dataset",
        "dct:identifier": table.get("fullyQualifiedName"),
        "dct:title": table.get("name"),
        "dct:description": table.get("description"),
        "dct:issued": table.get("createdAt"),
        "dct:modified": table.get("updatedAt"),
        "dcat:keyword": [tag["tagFQN"] for tag in table.get("tags", [])],
        "dct:publisher": {
            "@type": "foaf:Agent",
            "foaf:name": table.get("owner", {}).get("displayName") or table.get("owner", {}).get("name")
        } if table.get("owner") else None,
        "om:columns": table.get("columns", []),
        "om:tableType": table.get("tableType"),
        "om:profile": table.get("profile"),
        "om:usageSummary": table.get("usageSummary"),
        "om:database": table.get("database", {}).get("name"),
        "om:schema": table.get("databaseSchema", {}).get("name"),
        "om:service": table.get("service", {}).get("name"),
        "om:serviceType": table.get("serviceType"),
        "om:fullyQualifiedName": table.get("fullyQualifiedName"),
        "om:sourceUrl": table.get("sourceUrl"),
        "om:owner": table.get("owner"),
        "om:id": table.get("id")
    }

    return {k: v for k, v in dcat.items() if v is not None}
