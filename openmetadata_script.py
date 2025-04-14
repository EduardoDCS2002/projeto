# pip install requests python-dotenv

import requests
import json
from urllib.parse import urljoin

# Configurações iniciais
BASE_URL = "http://localhost:8585/api/v1"
AUTH_TOKEN = "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImFkbWluIiwicm9sZXMiOlsiQWRtaW4iXSwiZW1haWwiOiJhZG1pbkBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90IjpmYWxzZSwidG9rZW5UeXBlIjoiT01fVVNFUiIsImlhdCI6MTc0NDY0MzcxNiwiZXhwIjoxNzQ0NjQ3MzE2fQ.TwJbmuqBpp4bnstS6Pff1uzOw1YWDf9kR0hoU_4odxb3S119gxO5ndLKhP3unVcN7UqywOKPJNP5QHmDhn5uF0OWQrdCjsAGCIH_J9Hmw6OB0EPgrK02nFPsiiySOAbj3OPnVDciO5vEMJJNdHXKG4yfMl2spoAQanrYjb5qOmqc-pbmdRcBqLSnRecHW6_GdkWxig7-QlsTVXWQx_h1qe7zfvERHIwiCfXTzHagp3czokGgeACQoti6u0LbeBd7baK7f4pZm8KOgD8BmVyI-r7tc0j1wc0LE_ns8sMAicmz0B_qv5HazcEZ-0N6CH7R28fWEJf_FJbXmaF099bVnA"

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {AUTH_TOKEN}"
}

# Função genérica de requisição
def fazer_requisicao(method, endpoint, **kwargs):
    url = urljoin(BASE_URL, endpoint)
    response = requests.request(method, url, headers=HEADERS, **kwargs)
    response.raise_for_status()
    return response.json()

# Listar entidades (ex: tabelas)
def listar_entidades(tipo_entidade, limit=100, campos=None):
    params = {"limit": limit}
    if campos:
        params["fields"] = ",".join(campos)
    return fazer_requisicao("GET", f"/{tipo_entidade}", params=params)

# Obter detalhes de uma entidade
def obter_entidade(tipo_entidade, id_entidade, campos=None):
    params = {}
    if campos:
        params["fields"] = ",".join(campos)
    return fazer_requisicao("GET", f"/{tipo_entidade}/{id_entidade}", params=params)

# Criar nova entidade
def criar_entidade(tipo_entidade, dados):
    return fazer_requisicao("POST", f"/{tipo_entidade}", json=dados)

# Atualizar entidade
def atualizar_entidade(tipo_entidade, id_entidade, dados):
    return fazer_requisicao("PUT", f"/{tipo_entidade}/{id_entidade}", json=dados)

# Matar entidade
def deletar_entidade(tipo_entidade, id_entidade):
    return fazer_requisicao("DELETE", f"/{tipo_entidade}/{id_entidade}")

# Procurar entidades por termo
def buscar_entidades(termo, tipo_entidade=None):
    params = {"q": termo}
    if tipo_entidade:
        params["entityType"] = tipo_entidade
    return fazer_requisicao("GET", "/search/query", params=params)

# Listar serviços
def listar_servicos(tipo_servico):
    return fazer_requisicao("GET", f"/services/{tipo_servico}")

# Criar ou atualizar tabela
def criar_ou_atualizar_tabela(servico_bd, banco, schema, dados_tabela):
    dados_tabela.update({
        "service": servico_bd,
        "database": banco,
        "databaseSchema": schema
    })
    return fazer_requisicao("POST", "/tables", json=dados_tabela)

# Execução principal
if __name__ == "__main__":
    # Listar tabelas
    print("\n--Listar tabelas--\n")
    tabelas = listar_entidades("tables", limit=5)
    print("Tabelas encontradas:", json.dumps(tabelas, indent=2))

    # Procurar entidades que contém 'customer'
    print("\n--Procurar entidades--\n")
    resultados = buscar_entidades("customer", tipo_entidade="table")
    print("Resultados da busca:", json.dumps(resultados, indent=2))

    # Listar serviços de BD
    print("\n--Listar serviços de BD--\n")
    servicos = listar_servicos("database")
    print("Serviços encontrados:", json.dumps(servicos, indent=2))
