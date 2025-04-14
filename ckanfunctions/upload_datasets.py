import upload_dataset_to_ckan
import requests

CKAN_URL = "http://localhost:5000/api/3/action/"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJtSDF4V2pmQkEtMXRJZXNMRGZTaEk4U0dPX0JfdEtaVi1jSHBIeVJfcTJRIiwiaWF0IjoxNzQ0Mjk4MTIyfQ.7yBFMAk28YDFdq39PHnHPPJDaQxtRWxmKsiU0p4x6cc"
DATA_GOV_API = "https://catalog.data.gov/api/3/action/"

def search_datagov(query="", limit=100):
    # Search data.gov for datasets matching a query
    try:
        response = requests.get(
            f"{DATA_GOV_API}package_search",
            params={
                "q": query,
                "rows": limit,
                "sort": "metadata_created desc"
            },
            timeout=15
        )
        response.raise_for_status()
        return response.json()["result"]["results"]
    except Exception as e:
        print(f"Failed to search data.gov: {str(e)}")
        return []

def sync_all_from_datagov(query="", limit=10):
    # Sync all datasets from data.gov matching a query to local CKAN
    datasets = search_datagov(query, limit)
    if not datasets:
        print("No datasets found on data.gov or search failed.")
        return []

    results = []
    total = len(datasets)
    print(f"Found {total} datasets. Starting sync...")
    
    for i, dataset in enumerate(datasets, 1):
        print(f"\n[{i}/{total}] Processing: {dataset['name']}")
        try:
            result = upload_dataset_to_ckan.sync_dataset(
                source_url=DATA_GOV_API,
                target_ckan_url=CKAN_URL,
                target_api_key=API_KEY,
                dataset_name=dataset["name"]
            )
            results.append(result)
            print(f" Status: {result.get('status', 'unknown')}")
            if result.get("resources_added"):
                print(f"   + Added resources: {len(result['resources_added'])}")
        except Exception as e:
            results.append({
                "status": "failed",
                "dataset_name": dataset["name"],
                "error": str(e)
            })
            print(f" Failed: {str(e)}")

    # Print final result
    success = sum(1 for r in results if r.get("status") == "created") + sum(1 for r in results if r.get("status") == "updated")
    print(f"\n Summary:")
    print(f" Success: {success}/{total}")
    print(f" Failures: {total - success}")
    
    if total - success > 0:
        print("\nFailed datasets:")
        for r in results:
            if r.get("status") == "failed":
                print(f"- {r['dataset_name']}: {r['error']}")

    return results

# Example to run this with questions to the user:
if __name__ == "__main__":
    queryuser = input("Enter search query (or press Enter for all datasets): ").strip()
    limituser = input("Enter maximum datasets to sync (default 20): ").strip()
    
    sync_all_from_datagov(
        query=queryuser if queryuser else "",
        limit=int(limituser) if limituser.isdigit() else 20
    )