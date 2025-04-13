import os
import requests
from typing import Optional, Dict, Any

# Configurações (melhor usar variáveis de ambiente)
EMAIL = os.getenv("OM_EMAIL", "admin@open-metadata.org")
PASSWORD = os.getenv("OM_PASSWORD", "admin")
BASE_URL = os.getenv("OM_BASE_URL", "http://localhost:8585")
API_VERSION = "v1"

def obter_token(email: str, senha: str) -> Optional[str]:
    """Obtém token JWT para autenticação na API."""
    url = f"{BASE_URL}/api/{API_VERSION}/users/login"
    payload = {
        "email": email,
        "password": senha
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        return response.json().get("token")
    except requests.exceptions.RequestException as e:
        print(f" Erro ao fazer login: {str(e)}")
        return None

def listar_datasets(token: str, limit: int = 100, offset: int = 0) -> bool:
    """Lista todos os datasets com paginação."""
    url = f"{BASE_URL}/api/{API_VERSION}/tables"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    params = {
        "limit": limit,
        "offset": offset
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        datasets = data.get("data", [])
        
        if not datasets:
            print("ℹ Nenhum dataset encontrado.")
            return False
            
        print(f" Total de datasets: {data.get('paging', {}).get('total', len(datasets))}")
        print(" Lista de datasets:")
        
        for dataset in datasets:
            print(f" {dataset.get('name')} - {dataset.get('fullyQualifiedName')}")
            
        return True
        
    except requests.exceptions.RequestException as e:
        print(f" Erro ao listar datasets: {str(e)}")
        if hasattr(e, 'response') and e.response:
            print(f"Detalhes do erro: {e.response.text}")
        return False

if __name__ == "__main__":
    if token := obter_token(EMAIL, PASSWORD):
        print(" Token obtido com sucesso!")
        listar_datasets(token)
    else:
        print("Falha na autenticação. Verifique suas credenciais.")