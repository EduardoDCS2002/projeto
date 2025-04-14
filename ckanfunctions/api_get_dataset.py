import requests
import json

# Define the dataset ID from data.gov
dataset_id = "crime-data-from-2020-to-present"

# CKAN API endpoint for data.gov
data_gov_api_url = "https://catalog.data.gov/api/3/action/package_show"

# My CKAN instance details
ckan_api_url = "http://localhost:5000/api/3/action/resource_create"
api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJqNnFrQ3dhcVotM1dVLXEyYm9WbDNfRjQ1Z2lBVXBFT3BvRmlrYVFudFEwIiwiaWF0IjoxNzQzNTMyNTk1fQ.kp_u89vzmpXrMqJEDTQc6a3xIkbW7lhGTP5C91uyCEg"
ckan_dataset_id = "api_created_dataset"

# Fetch dataset metadata from data.gov
response = requests.get(f"{data_gov_api_url}?id={dataset_id}")
if response.status_code == 200:
    dataset_metadata = response.json()
    print("Dataset Metadata:")
    print(json.dumps(dataset_metadata, indent=2))

    # Extract resources (data files)
    resources = dataset_metadata["result"]["resources"]
    print("Resources:", resources)  

    # Fetch the existing dataset metadata from your CKAN instance
    package_show_url = f"{ckan_api_url.replace('resource_create', 'package_show')}"
    package_response = requests.get(
        package_show_url,
        headers={"Authorization": api_key},
        params={"id": ckan_dataset_id},
    )
    if package_response.status_code != 200:
        print(f"Failed to fetch dataset metadata from CKAN. Status code: {package_response.status_code}")
        exit()

    existing_resources = package_response.json()["result"]["resources"]

    for resource in resources:
        print(f"\nResource Name: {resource.get('name', 'Unnamed Resource')}")
        
        # Check if the resource has a URL
        if "url" not in resource:
            print(f"Skipping resource {resource.get('name', 'Unnamed Resource')} (no URL found).")
            continue

        resource_url = resource["url"]
        print(f"Resource URL: {resource_url}")

        # Check if the resource already exists in the CKAN dataset
        resource_exists = any(
            existing_resource["url"] == resource_url for existing_resource in existing_resources
        )
        if resource_exists:
            print(f"Resource with URL {resource_url} already exists in the dataset. Skipping upload.")
            continue

        # Upload the resource to your CKAN instance
        print(f"Uploading {resource.get('name', 'Unnamed Resource')} to CKAN...")
        try:
            # Download the resource directly from the URL
            file_response = requests.get(resource_url, timeout=10)  # Add timeout
            if file_response.status_code == 200:
                # Upload the resource to CKAN
                upload_response = requests.post(
                    ckan_api_url,
                    headers={"Authorization": api_key},
                    files=[("upload", (resource_url.split("/")[-1], file_response.content))],
                    data={
                        "package_id": ckan_dataset_id,  # ID of the dataset in your CKAN instance
                        "name": resource.get("name", resource_url.split("/")[-1]),  # Name of the resource
                        "url": resource_url,  # Original URL of the resource
                        "format": resource.get("format", ""),  # File format
                    },
                )
                if upload_response.status_code == 200:
                    print(f"Uploaded {resource.get('name', 'Unnamed Resource')} to CKAN successfully!")
                else:
                    print(f"Failed to upload {resource.get('name', 'Unnamed Resource')} to CKAN. Status code: {upload_response.status_code}")
                    print("Response:", upload_response.json())
            else:
                print(f"Failed to download {resource_url}. Status code: {file_response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Error uploading {resource.get('name', 'Unnamed Resource')}: {e}")
else:
    print(f"Failed to fetch metadata for dataset {dataset_id}. Status code: {response.status_code}")