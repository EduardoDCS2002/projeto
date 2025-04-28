import requests
import json
import base64
from urllib.parse import urljoin

# Initial settings 
BASE_URL = "http://localhost:8585/api/v1/"

# Get the AUTH_TOKEN for any machine running this (it's safer this way)
urlgettoken = BASE_URL + "users/login"

headers = {
    "Content-Type": "application/json"
}
encoded_password = base64.b64encode("admin".encode('utf-8')).decode('utf-8')
payload = { 
    "email": "admin@open-metadata.org",
    "password": encoded_password
}

AUTH_TOKEN = requests.post(urlgettoken, headers=headers, json=payload).json()["accessToken"]

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {AUTH_TOKEN}"
}

# Request function
def fazer_requisicao(method, endpoint, **kwargs):
    url = urljoin(BASE_URL, endpoint)
    response = requests.request(method, url, headers=HEADERS, **kwargs)
    response.raise_for_status()
    return response.json()

# List entities 
def listar_entidades(tipo_entidade, limit=100, campos=None):
    params = {"limit": limit}
    if campos:
        params = params + campos
    return fazer_requisicao("GET", tipo_entidade, params=params)

# Details of the entity
def obter_entidade(tipo_entidade, id_entidade, campos=None):
    if campos:
        params = campos
    return fazer_requisicao("GET", f"{tipo_entidade}/{id_entidade}", params=params)

# Create a new entity
def criar_entidade(tipo_entidade, dados):
    return fazer_requisicao("POST", tipo_entidade, json=dados)

# Update entity
def atualizar_entidade(tipo_entidade, id_entidade, dados):
    return fazer_requisicao("PUT", f"{tipo_entidade}/{id_entidade}", json=dados)

# Destroy/Kill entity
def eliminar_entidade(tipo_entidade, id_entidade):
    return fazer_requisicao("DELETE", f"{tipo_entidade}/{id_entidade}")

# Search entity by term
def procurar_entidades(termo, tipo_entidade=None):
    params = {"q": termo}
    if tipo_entidade:
        params["entityType"] = tipo_entidade
    return fazer_requisicao("GET", "search/query", params=params)

# List services
def listar_servicos(tipo_servico):
    return fazer_requisicao("GET", f"services/{tipo_servico}")

# Create or update table
def criar_ou_atualizar_tabela(servico_bd, banco, schema, dados_tabela):
    dados_tabela.update({
        "service": servico_bd,
        "database": banco,
        "databaseSchema": schema
    })
    return fazer_requisicao("POST", "tables", json=dados_tabela)


if __name__ == "__main__":
    # List tabels
    print("\n--Listar tabelas--\n")
    tabelas = listar_entidades("tables", limit=5)
    print("Tabelas encontradas:", json.dumps(tabelas, indent=2))

    # Search entities that contain 'customer'
    print("\n--Procurar entidades--\n")
    resultados = procurar_entidades("customer", tipo_entidade="table")
    print("Resultados da busca:", json.dumps(resultados, indent=2))

    # List database services
    print("\n--Listar serviços de BD--\n")
    servicos = listar_servicos("databaseServices")
    print("Serviços encontrados:", json.dumps(servicos, indent=2))

    print("\n--")

