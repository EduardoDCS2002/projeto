import requests
import time
from urllib.parse import urljoin
import re
# Configuration
CKAN_URL = "http://localhost:5000/api/3/action/"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJkcEJJRmkzVmlSVWVoSDhpLWQwcW9nekxPdGtpblV2RDR2SkFGbUp3NkVvIiwiaWF0IjoxNzQ0OTcyNDkwfQ.VoIeReQkmvJurqD_Q1CXp4-hr90bJatQDy4hBf8b7YU"
DATA_GOV_API = "https://catalog.data.gov/api/3/action/"

# ---- Helper Functions ----

def sanitize_tags(tags):
    """Clean up tag names to conform to CKAN tag naming rules."""
    valid_tags = []
    for tag in tags:
        name = tag["name"] if isinstance(tag, dict) else tag
        # Replace invalid characters with hyphen or remove them
        cleaned = re.sub(r"[^a-zA-Z0-9 ._-]", "", name)
        cleaned = cleaned.strip()
        if cleaned:
            valid_tags.append({"name": cleaned})
    return valid_tags

def ckan_get(ckan_url, action, api_key=None, params=None):
    """Generic GET request for CKAN API with 404 handling."""
    url = urljoin(ckan_url, action)
    headers = {"Authorization": api_key} if api_key else {}
    response = requests.get(url, params=params, headers=headers, timeout=15)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            raise FileNotFoundError(f"CKAN resource not found: {url}")
        raise
    data = response.json()
    if not data.get('success'):
        raise ValueError(f"API error: {data.get('error', 'Unknown error')}")
    return data

def ckan_post(ckan_url, action, json_data, api_key=None):
    """Generic POST request for CKAN API."""
    url = urljoin(ckan_url, action)
    headers = {"Authorization": api_key} if api_key else {}
    response = requests.post(url, json=json_data, headers=headers, timeout=15)
    response.raise_for_status()
    data = response.json()
    if not data.get('success'):
        raise ValueError(f"API error: {data.get('error', 'Unknown error')}")
    return data

def ensure_entity(ckan_url, api_key, entity_type, name, title=None, description="", extras=None):
    """Ensure an organization or group exists."""
    try:
        ckan_get(ckan_url, f"{entity_type}_show", api_key, {"id": name})
        return name
    except FileNotFoundError:
        create_data = {
            "name": name,
            "title": title or name.replace("-", " ").title(),
            "description": description,
            **(extras if extras else {})
        }
        new_entity = ckan_post(ckan_url, f"{entity_type}_create", create_data, api_key)
        return new_entity["result"]["name"]

def sync_dataset(source_dataset, target_ckan_url, target_api_key): # can be used in the convertor
    """Sync a dataset from data.gov (already retrieved) to local CKAN."""
    result = {
        "status": "skipped",
        "dataset_name": source_dataset.get("name", "unknown"),
        "resources_added": [],
        "errors": []
    }

    try:
        required_fields = ["title", "name", "organization"]
        for field in required_fields:
            if not source_dataset.get(field):
                raise ValueError(f"Source dataset missing required field: {field}")

        dataset_name = source_dataset["name"]

        # Check if it exists locally
        target_exists = False
        try:
            ckan_get(target_ckan_url, "package_show", target_api_key, {"id": dataset_name})
            target_exists = True
            result["status"] = "updated"
        except FileNotFoundError:
            result["status"] = "created"

        # Ensure organization exists
        org = source_dataset["organization"]
        org_name = ensure_entity(
            target_ckan_url, target_api_key,
            "organization", org["name"],
            org.get("title"), org.get("description", ""),
            extras={"image_url": org.get("image_url"), "approval_status": "approved"}
        )

        # Prepare dataset metadata
        EXCLUDE_FIELDS = {
            "id", "metadata_created", "metadata_modified", "revision_id",
            "creator_user_id", "private", "state",
            "resources", "organization", "tags", "groups"
        }

        package = {
            "name": dataset_name,
            "title": source_dataset["title"],
            "owner_org": org_name,
            "state": "active",
            "notes": source_dataset.get("notes", ""),
            "tags": sanitize_tags(source_dataset.get("tags", [])),
            "groups": source_dataset.get("groups", []),
            "extras": source_dataset.get("extras", [])
        }

        for k, v in source_dataset.items():
            if k not in EXCLUDE_FIELDS and k not in package:
                package[k] = v

        # Create or update
        if target_exists:
            ckan_post(target_ckan_url, "package_update", package, target_api_key)
        else:
            ckan_post(target_ckan_url, "package_create", package, target_api_key)

        # Add resources
        for resource in source_dataset.get("resources", []):
            if not resource.get("url"):
                result["errors"].append(f"Resource missing URL: {resource.get('id')}")
                continue

            time.sleep(0.5)  # Rate limit

            try:
                ckan_post(
                    target_ckan_url, "resource_create",
                    {
                        "package_id": package["name"],
                        "url": resource["url"],
                        "name": resource.get("name", resource["url"].split("/")[-1]),
                        "format": resource.get("format", "").upper(),
                        "description": resource.get("description", "")
                    },
                    target_api_key
                )
                result["resources_added"].append(resource["url"])
            except Exception as e:
                result["errors"].append(f"Resource {resource.get('url')} failed: {str(e)}")

        if result["errors"]:
            result["status"] = "partially_synced"

    except Exception as e:
        result["status"] = "failed"
        result["errors"].append(str(e))

    return result

def search_datagov(query="", limit=10):
    """Search for datasets on data.gov."""
    try:
        response = requests.get(
            f"{DATA_GOV_API}package_search",
            params={"q": query, "rows": limit},
            timeout=15
        )
        response.raise_for_status()
        return response.json()["result"]["results"]
    except Exception as e:
        print(f"Failed to search data.gov: {str(e)}")
        return []

def sync_all_from_datagov(query="", limit=10):
    """Sync multiple datasets from data.gov to local CKAN."""
    datasets = search_datagov(query, limit)
    if not datasets:
        print("No datasets found on data.gov or search failed.")
        return []

    results = []
    print(f"Found {len(datasets)} datasets. Starting sync...")

    for dataset in datasets:
        print(f"\nProcessing: {dataset['name']}")
        try:
            result = sync_dataset(
            source_dataset=dataset,
            target_ckan_url=CKAN_URL,
            target_api_key=API_KEY
        )


            print(f"Status: {result.get('status', 'unknown')}")
            results.append(result)
            time.sleep(1)
        except Exception as e:
            print(f"Failed: {str(e)}")
            results.append({
                "status": "failed",
                "dataset_name": dataset["name"],
                "errors": [str(e)]
            })

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

# ---- Entry Point ----

if __name__ == "__main__":
    query = input("Enter search query (or press Enter for all datasets): ").strip()
    limit = input("Enter maximum datasets to sync (default 20): ").strip()

    sync_all_from_datagov(
        query=query if query else "",
        limit=int(limit) if limit.isdigit() else 20
    )
