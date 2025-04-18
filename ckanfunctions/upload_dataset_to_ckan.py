import requests
from urllib.parse import urljoin
import time

def ckan_get(ckan_url, action, api_key=None, params=None):
    """Generic GET request for CKAN API"""
    url = urljoin(ckan_url, action)
    headers = {"Authorization": api_key} if api_key else {}
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
    """Generic POST request for CKAN API"""
    url = urljoin(ckan_url, action)
    headers = {"Authorization": api_key} if api_key else {}
    try:
        response = requests.post(url, json=json_data, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        if not data.get('success'):
            raise ValueError(f"API error: {data.get('error', 'Unknown error')}")
        return data
    except requests.exceptions.RequestException as e:
        raise ValueError(f"CKAN API ({action}) failed: {str(e)}")

def ensure_entity(ckan_url, api_key, entity_type, name, title=None, description="", extras=None):
    """Ensure an organization/group exists, create if missing"""
    try:
        existing = ckan_get(ckan_url, f"{entity_type}_show", api_key, {"id": name})
        return name
    except ValueError as e:
        if "404" in str(e) or "Not found" in str(e):
            create_data = {
                "name": name,
                "title": title or name.replace("-", " ").title(),
                "description": description,
                **(extras if extras else {})
            }
            new_entity = ckan_post(ckan_url, f"{entity_type}_create", create_data, api_key)
            return new_entity["result"]["name"]
        raise

def ensure_tag(ckan_url, api_key, tag_name):
    """Ensure a tag exists, create if missing"""
    try:
        ckan_get(ckan_url, "tag_show", api_key, {"id": tag_name})
        return tag_name
    except ValueError:
        ckan_post(ckan_url, "tag_create", {"name": tag_name}, api_key)
        return tag_name

def sync_dataset(source_url, target_ckan_url, target_api_key, dataset_name):
    """Sync a dataset from source to target CKAN instance"""
    result = {
        "status": "skipped",
        "dataset_name": dataset_name,
        "resources_added": [],
        "errors": []
    }

    try:
        # Fetch source dataset
        source_dataset = ckan_get(source_url, "package_show", params={"id": dataset_name})["result"]
        
        # Validate required fields
        required_fields = ["title", "name", "organization"]
        for field in required_fields:
            if not source_dataset.get(field):
                raise ValueError(f"Source dataset missing required field: {field}")

        # Check if target exists
        target_exists = False
        try:
            target_dataset = ckan_get(target_ckan_url, "package_show", target_api_key, {"id": dataset_name})["result"]
            target_exists = True
            result["status"] = "updated"
        except ValueError as e:
            if "Not found" not in str(e) and "404" not in str(e):
                raise
            result["status"] = "created"

        # Sync organization
        org = source_dataset["organization"]
        org_extras = {
            "image_url": org.get("image_url"),
            "approval_status": "approved"
        }
        org_name = ensure_entity(
            target_ckan_url, target_api_key,
            "organization", org["name"],
            org.get("title"), org.get("description", ""),
            extras=org_extras
        )

        # Prepare dataset data
        EXCLUDE_FIELDS = {
            "id",  # Critical: Never force an external ID
            "metadata_created", "metadata_modified", "revision_id",
            "creator_user_id", "private", "state",
            "resources", "organization", "tags", "groups"
        }

        package = {
            "name": dataset_name,
            "title": source_dataset["title"],
            "owner_org": org_name,
            "state": "active",
            "notes": source_dataset.get("notes", ""),
            "tags": [{"name": t["name"]} 
                    for t in source_dataset.get("tags", [])],
            "extras": source_dataset.get("extras", [])
        }

        # Add remaining fields
        for k, v in source_dataset.items():
            if k not in EXCLUDE_FIELDS and k not in package:
                package[k] = v

        # Create or update package
        if target_exists:
            response = ckan_post(target_ckan_url, "package_update", package, target_api_key)
        else:
            response = ckan_post(target_ckan_url, "package_create", package, target_api_key)
            result["status"] = "created"

        # Sync resources (with rate limiting)
        for resource in source_dataset.get("resources", []):
            if not resource.get("url"):
                result["errors"].append(f"Resource missing URL: {resource.get('id')}")
                continue

            time.sleep(0.5)  # Rate limiting

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