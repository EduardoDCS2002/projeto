def dcat_to_openmetadata(dcat_dataset: dict, openmetadata_url: str, token: str) -> dict:
    """
    Cria ou atualiza uma tabela no OpenMetadata a partir de um DCAT JSON-LD.
    Garante o reaproveitamento de todos os campos possíveis.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    fqn = dcat_dataset["dct:identifier"]
    fqn_parts = fqn.split(".")
    if len(fqn_parts) != 4:
        raise ValueError("FQN inválido. Esperado formato: service.database.schema.table")

    service_name, database_name, schema_name, table_name = fqn_parts

    # Recuperar o schema para obter o ID
    schema_url = f"{openmetadata_url}/databaseSchemas/name/{service_name}.{database_name}.{schema_name}"
    schema_response = requests.get(schema_url, headers=headers)
    schema_response.raise_for_status()
    schema_id = schema_response.json()["id"]

    payload = {
        "name": table_name,
        "description": dcat_dataset.get("dct:description"),
        "fullyQualifiedName": fqn,
        "tableType": dcat_dataset.get("om:tableType", "Regular"),
        "tags": [{"tagFQN": tag} for tag in dcat_dataset.get("dcat:keyword", [])],
        "columns": dcat_dataset.get("om:columns", []),
        "profile": dcat_dataset.get("om:profile"),
        "usageSummary": dcat_dataset.get("om:usageSummary"),
        "databaseSchema": {
            "id": schema_id,
            "type": "databaseSchema"
        },
        "owner": dcat_dataset.get("om:owner"),
        "sourceUrl": dcat_dataset.get("om:sourceUrl")
    }

    # Criação da tabela
    create_url = f"{openmetadata_url}/tables"
    response = requests.post(create_url, headers=headers, json=payload)
    
    if response.status_code == 409:
        print(f"Tabela já existe, a tentar atualizar: {fqn}")
        update_url = f"{openmetadata_url}/tables/name/{fqn}"
        response = requests.put(update_url, headers=headers, json=payload)

    response.raise_for_status()
    return response.json()
