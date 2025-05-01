# Interoperabilidade de catálogo de metadados: OpenMatadata com CKAN

Qualquer organização que faça a gestão de muitos conjuntos de dados precisa de equipa e de ferramentas que apoiem a governança dos dados. Um catálogo de metadados é uma das componentes chave para suportar a governança de dados.
No catálogo de metadados constam os dados sobre os dados, tal como o catálogo bibliotecário tem os dados sobre os livros.
O OpenMetadata, o CKAN, o Apache Atlas ou o Amundsen são exemplos de ferramentas que implementam catálogos de metadados. Na verdade, todas elas estendem a noção de catálogo e suportam muitas outras funcionalidades.
Neste projeto, pretende-se pegar em duas destas ferramentas, que são open source, e permitir que só registos de uma e de outra possam ser trocados, sem perdas de informação.
Com este trabalho, os utilizadores de OpenMetadata podem passar os registos para CKAN e vice versa.

O teste mais simples da utilização assumindo que tem o ckan e o openmetadata operacional e a correr, é correr o script convertor/main.py
