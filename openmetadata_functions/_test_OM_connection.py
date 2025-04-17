from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)

server_config = OpenMetadataConnection(
    hostPort="http://localhost:8585/api",
    authProvider="openmetadata",
    securityConfig={"jwtToken": "your-jwt-token-here"},
)

metadata = OpenMetadata(server_config)

try:
    version = metadata.get_server_version()
    print(f"Successfully connected to OpenMetadata! Version: {version}")
except Exception as e:
    print(f"Connection failed: {e}")