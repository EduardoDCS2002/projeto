import requests

def dcat_to_openmetadata(dcat_dataset: dict, openmetadata_url: str, token: str) -> dict:
    """
    Cria uma nova tabela no OpenMetadata a partir de um DCAT.
    Assume que o serviço, database e schema já existem.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Requisitos: Fully Qualified Name deve ser composto: service.db.schema.table
    fqn_parts = dcat_dataset["dct:identifier"].split(".")
    if len(fqn_parts) != 4:
        raise ValueError("FQN inválido. Esperado formato: service.db.schema.table")

    service_name, database_name, schema_name, table_name = fqn_parts

    payload = {
        "name": table_name,
        "tableType": dcat_dataset.get("om:tableType", "Regular"),
        "description": dcat_dataset.get("dct:description"),
        "tags": [{"tagFQN": tag} for tag in dcat_dataset.get("dcat:keyword", [])],
        "columns": dcat_dataset.get("om:columns", []),
        "databaseSchema": {
            "id": "",  # ← precisa do ID real
            "type": "databaseSchema",
            "name": schema_name
        },
        "fullyQualifiedName": dcat_dataset["dct:identifier"]
    }

    # Passo extra: procurar o ID real do schema pelo nome
    schema_url = f"{openmetadata_url}/databaseSchemas/name/{service_name}.{database_name}.{schema_name}"
    schema_response = requests.get(schema_url, headers=headers)
    schema_response.raise_for_status()
    schema_id = schema_response.json()["id"]
    payload["databaseSchema"]["id"] = schema_id

    # Enviar ao OpenMetadata
    endpoint = f"{openmetadata_url}/tables"
    response = requests.post(endpoint, headers=headers, json=payload)
    response.raise_for_status()

    print(f"Tabela criada: {table_name}")
    return response.json()
