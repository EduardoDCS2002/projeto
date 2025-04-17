import requests
from urllib.parse import quote
from typing import List, Dict
import upload_to_openmetadata

DATA_GOV_API = "https://catalog.data.gov/api/3"
OPENMETADATA_URL = "http://localhost:8585/api/v1"

def search_datasets(query: str, max_results: int = 10) -> List[Dict]:
    # Search for datasets on data.gov matching the query
    url = f"{DATA_GOV_API}/action/package_search?q={quote(query)}&rows={max_results}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()["result"]["results"]

def process_datasets():
    # Process datasets after a query
    token = upload_to_openmetadata.get_auth_token()
    upload_to_openmetadata.setup_infrastructure(token)
    
    query = input("Make a query to search datasets: ")
    max_results = int(input("How many datasets do you want? "))
    datasets = search_datasets(query, max_results)
    
    total_created = 0
    total_skipped = 0
    
    for dataset in datasets:
        dataset_id = dataset["id"]
        print(f"\nProcessing dataset: {dataset['title']} (ID: {dataset_id})")
        created = 0
        skipped = 0
        
        try:
            dataset_meta = upload_to_openmetadata.get_dataset_metadata(dataset_id)
            
            for resource in dataset_meta.get("resources", []):
                if not resource.get("url"):
                    continue
                
                print(f"Processing resource: {resource.get('name', resource['id'])}")
                try:
                    result = upload_to_openmetadata.create_table(token, dataset_meta, resource)
                    if result.get("status") == "already_exists":
                        print("Table already exists - skipping")
                        skipped += 1
                    else:
                        created += 1
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 409:
                        print("Table already exists - skipping")
                        skipped += 1
                    else:
                        print(f"Failed to create table: {str(e)}")
                        skipped += 1
                except Exception as e:
                    print(f"Error creating table: {str(e)}")
                    skipped += 1
            
            print(f"Created {created} new tables, skipped {skipped} existing tables")
            total_created += created
            total_skipped += skipped
            
        except Exception as e:
            print(f"Error processing dataset {dataset_id}: {str(e)}")
            continue
    
    print(f"\nProcessing complete. Total created: {total_created}, Total skipped: {total_skipped}")

if __name__ == "__main__":
    process_datasets()