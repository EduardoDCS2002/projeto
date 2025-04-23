def openmetadata_to_dcat(table: dict) -> dict:
    """
    Converte um dataset do OpenMetadata para um objeto JSON-LD baseado no DCAT,
    incluindo extensões para preservar informações ricas.
    """
    dcat = {
        "@context": "https://www.w3.org/ns/dcat.jsonld",
        "@type": "dcat:Dataset",
        "dct:title": table.get("name"),
        "dct:description": table.get("description"),
        "dct:identifier": table.get("fullyQualifiedName"),
        "dct:publisher": {
            "@type": "foaf:Agent",
            "foaf:name": table.get("owner", {}).get("displayName") or table.get("owner", {}).get("name")
        } if table.get("owner") else None,
        "dcat:keyword": [tag["tagFQN"] for tag in table.get("tags", [])],
        "dct:issued": table.get("createdAt"),
        "dct:modified": table.get("updatedAt"),
        "dcat:distribution": [],
        "om:columns": table.get("columns", []),  # Extensão para manter colunas
        "om:tableType": table.get("tableType"),
        "om:service": table.get("service", {}).get("name"),
        "om:database": table.get("database", {}).get("name"),
        "om:schema": table.get("databaseSchema", {}).get("name"),
        "om:usageSummary": table.get("usageSummary"),
        "om:profile": table.get("profile")
    }

    # Remover campos None
    dcat = {k: v for k, v in dcat.items() if v is not None}

    return dcat
