import json
import os

def save_dcat_to_file(dcat_dataset: dict, directory: str = "exported_dcats") -> str:
    """
    Guarda o objeto DCAT em formato .jsonld num diretório local.
    Retorna o caminho completo do arquivo guardado.
    """
    os.makedirs(directory, exist_ok=True)

    dataset_id = dcat_dataset.get("dct:identifier", "dataset")
    filename = f"{dataset_id}.jsonld"
    filepath = os.path.join(directory, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(dcat_dataset, f, indent=2, ensure_ascii=False)

    print(f"DCAT salvo em: {filepath}")
    return filepath
