# pip install requests python-dotenv 

import requests
import json
from typing import Dict, List, Optional
from urllib.parse import urljoin

# Cliente para interagir com a API
class OpenMetadataClient:
    
    # Inicializa o cliente com configurações de conexão
    def __init__(self, base_url: str = "http://localhost:8585/api/v1", auth_token: str = ""):
        if not base_url.endswith('/api/v1'):
            base_url = base_url.rstrip('/') + '/api/v1'
        self.base_url = base_url
        self.auth_token = auth_token
        self.headers = {
            "Content-Type":"application/json",
            "Authorization":f"Bearer {auth_token}"
        }
    
    # Método interno para fazer requisições HTTP à API
    def _request(self, method: str, endpoint: str, **kwargs) -> Dict:
        # method: GET/POST/PUT/DELETE
        # endpoint: endpoint da API
        # kwargs: Argumentos adicionais para requests.request()

        url = urljoin(self.base_url, endpoint)
        response = requests.request(method, url, headers=self.headers, **kwargs)
        response.raise_for_status()
        
        return response.json() # Retorna o corpo da resposta como JSON
    
    # Lista entidades de um tipo específico.
    def list_entities(self, entity_type: str, limit: int = 100, fields: Optional[List[str]] = None) -> Dict:
        # entity_type: tables/topics...
        # limit: read again
        # fields: campos especificos

        params = {"limit": limit}  # Parâmetros básicos de paginação
        if fields:  # Se campos específicos foram solicitados, adiciona ao params
            params["fields"] = ",".join(fields)

        return self._request("GET", f"/{entity_type}", params=params)

    # Obtém detalhes de uma entidade
    def get_entity(self, entity_type: str, entity_id: str, fields: Optional[List[str]] = None) -> Dict:
        
        params = {}
        if fields:
            params["fields"] = ",".join(fields)

        return self._request("GET", f"/{entity_type}/{entity_id}", params=params)

    # Cria uma nova entidade
    def create_entity(self, entity_type: str, entity_data: Dict) -> Dict:
        # entity_data: Dicionário com os dados da entidade

        return self._request("POST", f"/{entity_type}", json=entity_data)

    # Atualiza uma entidade
    def update_entity(self, entity_type: str, entity_id: str, entity_data: Dict) -> Dict:

        return self._request("PUT", f"/{entity_type}/{entity_id}", json=entity_data)
    
    # Elimina uma entidade
    def delete_entity(self, entity_type: str, entity_id: str) -> Dict:

        return self._request("DELETE", f"/{entity_type}/{entity_id}")
    
    # Procura entidades usando uma query
    def search_entities(self, query: str, entity_type: Optional[str] = None) -> Dict:

        params = {"q": query}  # Parâmetro de query
        if entity_type:
            params["entityType"] = entity_type  # Filtro por tipo de entidade
        
        return self._request("GET", "/search/query", params=params) 

    # Lista serviços de um tipo
    def list_services(self, service_type: str) -> Dict:
        # service_type: Tipo de serviço (database,messaging...)

        return self._request("GET", f"/services/{service_type}")
    
    def create_or_update_table(self, database_service: str, database: str, schema: str, table_data: Dict) -> Dict:
        # database_service: Nome do serviço de BD
        # database: Nome da BD
        # schema: Nome do schema
        # table_data: Dados da tabela

        endpoint = f"/tables"
        # Adiciona os relacionamentos hierárquicos (tabela → schema → database → service)
        table_data.update({
            "service": database_service,
            "database": database,
            "databaseSchema": schema
        })
        
        return self._request("POST", endpoint, json=table_data)



if __name__ == "__main__":
    
    client = OpenMetadataClient(
        base_url= "http://localhost:8585/api/v1",
        auth_token= "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImFkbWluIiwicm9sZXMiOlsiQWRtaW4iXSwiZW1haWwiOiJhZG1pbkBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90IjpmYWxzZSwidG9rZW5UeXBlIjoiT01fVVNFUiIsImlhdCI6MTc0NDY0MzcxNiwiZXhwIjoxNzQ0NjQ3MzE2fQ.TwJbmuqBpp4bnstS6Pff1uzOw1YWDf9kR0hoU_4odxb3S119gxO5ndLKhP3unVcN7UqywOKPJNP5QHmDhn5uF0OWQrdCjsAGCIH_J9Hmw6OB0EPgrK02nFPsiiySOAbj3OPnVDciO5vEMJJNdHXKG4yfMl2spoAQanrYjb5qOmqc-pbmdRcBqLSnRecHW6_GdkWxig7-QlsTVXWQx_h1qe7zfvERHIwiCfXTzHagp3czokGgeACQoti6u0LbeBd7baK7f4pZm8KOgD8BmVyI-r7tc0j1wc0LE_ns8sMAicmz0B_qv5HazcEZ-0N6CH7R28fWEJf_FJbXmaF099bVnA"
    )

    # Listar tabelas
    print("\n--Listar tabelas--\n")
    tables = client.list_entities("tables", limit=5)
    print("Tabelas encontradas:", json.dumps(tables, indent=2))
    
    # Procurar entidades do tipo 'table' contendo 'customer'
    print("\n--Procurar entidades--\n")
    search_results = client.search_entities("customer", entity_type="table")
    print("Resultados da busca:", json.dumps(search_results, indent=2))
    
    # Listar serviços de bases de dados
    print("\n--Listando serviços de BD--")
    db_services = client.list_services("database")
    print("Serviços encontrados:", json.dumps(db_services, indent=2))