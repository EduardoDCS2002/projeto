import upload_dataset_to_ckan
import requests
import time

CKAN_URL = "http://localhost:5000/api/3/action/"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJkcEJJRmkzVmlSVWVoSDhpLWQwcW9nekxPdGtpblV2RDR2SkFGbUp3NkVvIiwiaWF0IjoxNzQ0OTcyNDkwfQ.VoIeReQkmvJurqD_Q1CXp4-hr90bJatQDy4hBf8b7YU"
DATA_GOV_API = "https://catalog.data.gov/api/3/action/"

def search_datagov(query="", limit=10):
    """Search data.gov"""
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
    """Main sync function with sequential processing"""
    datasets = search_datagov(query, limit)
    if not datasets:
        print("No datasets found on data.gov or search failed.")
        return []

    results = []
    print(f"Found {len(datasets)} datasets. Starting sync...")

    for dataset in datasets:
        print(f"\nProcessing: {dataset['name']}")
        try:
            result = upload_dataset_to_ckan.sync_dataset(
                source_url=DATA_GOV_API,
                target_ckan_url=CKAN_URL,
                target_api_key=API_KEY,
                dataset_name=dataset["name"]
            )
            print(f"Status: {result.get('status', 'unknown')}")
            results.append(result)
            time.sleep(1)  # Rate limiting between datasets
        except Exception as e:
            print(f"Failed: {str(e)}")
            results.append({
                "status": "failed",
                "dataset_name": dataset["name"],
                "error": str(e)
            })

    # Generate report
    success = sum(1 for r in results if r.get("status") in ("created", "updated"))
    failures = sum(1 for r in results if r.get("status") == "failed")
    partial = sum(1 for r in results if r.get("status") == "partially_synced")

    print(f"\nSummary:")
    print(f"Success: {success}")
    print(f"Partial: {partial}")
    print(f"Failures: {failures}")

    if failures > 0:
        print("\nFailed datasets:")
        for r in results:
            if r.get("status") == "failed":
                print(f"- {r['dataset_name']}: {r.get('errors', 'Unknown error')}")

    return results

if __name__ == "__main__":
    query = input("Enter search query (or press Enter for all datasets): ").strip()
    limit = input("Enter maximum datasets to sync (default 20): ").strip()

    sync_all_from_datagov(
        query=query if query else "",
        limit=int(limit) if limit.isdigit() else 20
    )