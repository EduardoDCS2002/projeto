import requests
import os
from time import sleep

def testar_conexao_openmetadata():
    # Configurações
    EMAIL = os.getenv("OM_EMAIL", "admin@open-metadata.org")
    PASSWORD = os.getenv("OM_PASSWORD", "admin")
    BASE_URL = os.getenv("OM_BASE_URL", "http://localhost:8585")
    HEALTH_CHECK_URL = f"{BASE_URL}/api/v1/system/config"
    LOGIN_URL = f"{BASE_URL}/api/v1/users/login"

    print("🔍 Testando conexão com o OpenMetadata...")

    # 1. Testar se o servidor está respondendo
    try:
        print(f"🔄 Verificando saúde da API em {HEALTH_CHECK_URL}...")
        health_response = requests.get(HEALTH_CHECK_URL, timeout=5)
        
        if health_response.status_code == 200:
            print("✅ Servidor está respondendo")
            print(f"📋 Versão do OpenMetadata: {health_response.json().get('version', 'desconhecida')}")
        else:
            print(f"❌ Servidor respondeu com status {health_response.status_code}")
            print("ℹ️ Verifique se o OpenMetadata está rodando corretamente")
            return False
    except requests.exceptions.ConnectionError:
        print("❌ Não foi possível conectar ao servidor")
        print("ℹ️ Verifique se:")
        print("- O OpenMetadata está rodando (docker ps)")
        print("- A URL está correta (deve ser http://localhost:8585 por padrão)")
        print("- Nenhum firewall está bloqueando a conexão")
        return False

    # 2. Testar autenticação
    print("\n🔑 Testando autenticação...")
    login_payload = {"email": EMAIL, "password": PASSWORD}
    
    try:
        login_response = requests.post(LOGIN_URL, json=login_payload, timeout=10)
        
        if login_response.status_code == 200:
            print("✅ Autenticação bem-sucedida!")
            return True
        else:
            print(f"❌ Falha na autenticação (status {login_response.status_code})")
            print("Resposta do servidor:", login_response.text)
            print("\nℹ️ Soluções possíveis:")
            print("- Verifique se as credenciais estão corretas")
            print("- Se você alterou a senha padrão, use a nova senha")
            print("- Execute 'docker-compose down && docker-compose up -d' para reiniciar")
            return False
            
    except Exception as e:
        print(f"❌ Erro durante autenticação: {str(e)}")
        return False

if __name__ == "__main__":
    # Tentar por 3 vezes com intervalo de 5 segundos
    max_tentativas = 3
    for tentativa in range(1, max_tentativas + 1):
        print(f"\nTentativa {tentativa} de {max_tentativas}")
        if testar_conexao_openmetadata():
            break
        if tentativa < max_tentativas:
            print(f"⏳ Aguardando 5 segundos antes de tentar novamente...")
            sleep(5)
    else:
        print("\n🔴 Todas as tentativas falharam. Verifique seu setup do OpenMetadata.")