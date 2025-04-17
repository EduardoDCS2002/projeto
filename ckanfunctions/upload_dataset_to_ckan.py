import requests
from urllib.parse import urljoin

def list_all_datasets(ckan_url, api_key=None, include_private=True):
    # List all datasets even hidden
    params = {
        "rows": 1000,  # Max allowed by CKAN
        "include_private": include_private
    }
    response = requests.get(
        urljoin(ckan_url, "package_search"),
        headers={"Authorization": api_key},
        params=params,
        timeout=15
    )
    response.raise_for_status()
    data = response.json()
    
    if not data.get("success"):
        raise ValueError(f"API error: {data.get('error')}")
    
    return [pkg["name"] for pkg in data["result"]["results"]]

'''
# It was necessary to fix an old error
# We don't have the courage to erase it :(

def delete_dataset(ckan_url, dataset_id_or_name, api_key): 
    # Kill a dataset from CKAN
    try:
        response = requests.post(
            urljoin(ckan_url, "package_delete"),
            headers={"Authorization": api_key},
            json={"id": dataset_id_or_name},
            timeout=15
        )
        response.raise_for_status()
        data = response.json()
        
        if not data.get("success"):
            raise ValueError(f"API error: {data.get('error', 'Unknown error')}")
        
        print(f"Successfully deleted dataset: {dataset_id_or_name}")
        return data["result"]
    
    except requests.exceptions.RequestException as e:
        raise ValueError(f"Failed to delete dataset: {str(e)}")
'''
        
# Get and Post

def ckan_get(ckan_url, action, api_key=None, params=None):
    # Generic GET request for CKAN API
    url = ckan_url + action
    headers = {"Authorization": api_key}
    try:
        response = requests.get(url, params=params, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        if not data.get('success'):
            raise ValueError(f"API error: {data.get('error', 'Unknown error')}")
        return data
    except requests.exceptions.RequestException as e:
        raise ValueError(f"CKAN API ({action}) failed: {str(e)}")

def ckan_post(ckan_url, action, json_data, api_key=None):
    # Generic POST request for CKAN API
    url = ckan_url + action
    headers = {"Authorization": api_key}
    try:
        response = requests.post(url, json=json_data, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        if not data.get('success'):
            raise ValueError(f"API error: {data.get('error', 'Unknown error')}")
        return data
    except requests.exceptions.RequestException as e:
        raise ValueError(f"CKAN API ({action}) failed: {str(e)}")

## Functions that need Get
def get_package_list(ckan_url, limit=10, api_key=None):
    # List all datasets (packages) in CKAN instance
    return ckan_get(ckan_url, "package_list", api_key, {"limit": limit})

def get_organization_list(ckan_url, api_key=None):
    # List all organizations
    return ckan_get(ckan_url, "organization_list", api_key)

def get_group_list(ckan_url, api_key=None):
    # List all groups
    return ckan_get(ckan_url, "group_list", api_key)

def get_tag_list(ckan_url, api_key=None):
    # List all tags
    return ckan_get(ckan_url, "tag_list", api_key)

def get_package_show(ckan_url, dataset_name, api_key=None):
    # Get full metadata for a specific dataset
    return ckan_get(ckan_url, "package_show", api_key, {"id": dataset_name})

def get_resource_show(ckan_url, resource_id, api_key=None):
    # Get metadata for a specific resource
    return ckan_get(ckan_url, "resource_show", api_key, {"id": resource_id})

# Check if entity exists or create it
def ensure_entity(ckan_url, api_key, entity_type, name, title=None, description=""):
    try:
        # Try to get existing
        existing = ckan_get(
            ckan_url, 
            f"{entity_type}_show", 
            api_key, 
            {"id": name}
        )
        return name
    except ValueError as e:
        if "404" in str(e) or "Not found" in str(e):
            # Create if doesn't exist
            create_data = {
                "name": name,
                "title": title or name.replace("-", " ").title(),
                "description": description
            }
            new_entity = ckan_post(
                ckan_url,
                f"{entity_type}_create",
                create_data,
                api_key
            )
            return new_entity["result"]["name"]
        raise

# sync dataset
def sync_dataset(source_url, target_ckan_url, target_api_key, dataset_name):

    result = {
        "status": "skipped",
        "dataset_name": dataset_name,
        "resources_added": [],
        "errors": []
    }

    try:
        # Fetch source dataset
        source_dataset = get_package_show(source_url, dataset_name)["result"]
        
        # Check target existence
        target_resources = {}
        target_dataset_exists = False

        try:
            target_dataset = get_package_show(target_ckan_url, dataset_name, target_api_key)["result"]
            target_resources = {r["url"]: r for r in target_dataset.get("resources", [])}
            result["status"] = "updated"                            # Will update existing dataset
            target_dataset_exists = True
        except ValueError as e:
            if "Not found" not in str(e) and "404" not in str(e):
                raise                                               # Re-raise if it's not a "not found" error
            # Dataset doesn't exist yet - this is OK!
            result["status"] = "created"                            # Will create new dataset

        # Check if organization exists
        org = source_dataset.get("organization")
        if not org:
            raise ValueError("Source dataset has no organization")
        
        org_name = ensure_entity(
            target_ckan_url, target_api_key,
            "organization", org["name"],
            org.get("title"), org.get("description", "")
        )

        # Prepare dataset to create or update
        EXCLUDE_FIELDS = {
            "metadata_created", "metadata_modified", "revision_id",
            "creator_user_id", "private", "state",
            "resources", "organization", "tags", "groups",
            "id", "name", "owner_org"
        }

        TRANSFORM_FIELDS = {
            "tags": lambda ts: [{"name": t["name"]} for t in ts],
            "groups": lambda gs: [{"name": g["name"]} for g in gs]
        }

        package = {
            "id": source_dataset["id"],
            "name": dataset_name,
            "owner_org": org_name,
            **{
                k: TRANSFORM_FIELDS[k](v) if k in TRANSFORM_FIELDS else v
                for k, v in source_dataset.items()
                if k not in EXCLUDE_FIELDS
            }
        }

        # Create or update package
        if target_dataset_exists:
            ckan_post(target_ckan_url, "package_update", package, target_api_key)
        else:
            print(package["id"])
            print(package["name"])
            ckan_post(target_ckan_url, "package_create", package, target_api_key)
            result["status"] = "created"

        # Sync resources
        for resource in source_dataset.get("resources", []):
            if not resource.get("url"):
                result["errors"].append(f"Resource missing URL: {resource.get('id')}")
                continue

            if resource["url"] in target_resources:
                continue  # Skip existing

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

    except Exception as e:
        result["status"] = "failed"
        result["errors"].append(str(e))
    
    return result