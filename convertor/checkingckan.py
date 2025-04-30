import requests
CKAN_URL = "http://localhost:5000/api/3/action/"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJtSDF4V2pmQkEtMXRJZXNMRGZTaEk4U0dPX0JfdEtaVi1jSHBIeVJfcTJRIiwiaWF0IjoxNzQ0Mjk4MTIyfQ.7yBFMAk28YDFdq39PHnHPPJDaQxtRWxmKsiU0p4x6cc"
DATA_GOV_API = "https://catalog.data.gov/api/3/action/"

def get_all_datasets_from_ckan(base_url, api_key):
    """
    Gets all datasets with full details including resources from a CKAN instance.
    Returns a dictionary where keys are organization names and values are lists of full datasets.
    """
    headers = {"Authorization": api_key}
    datasets_by_org = {}

    # Get all organizations
    orgs_url = f"{base_url}organization_list"
    orgs_response = requests.get(orgs_url, headers=headers)
    orgs_response.raise_for_status()
    organizations = orgs_response.json().get("result", [])

    # Get datasets for each organization
    for org in organizations:
        # First get the list of dataset IDs/names for this org
        org_packages_url = f"{base_url}package_search"
        params = {
            "q": f"organization:{org}",
            "rows": 1000,  # Get as many as possible in one request
            "fl": "id,name"  # Only get id and name to minimize response size
        }
        search_response = requests.get(org_packages_url, headers=headers, params=params)
        search_response.raise_for_status()
        datasets_list = search_response.json().get("result", {}).get("results", [])
        
        # Now get full details for each dataset
        full_datasets = []
        for dataset in datasets_list:
            dataset_url = f"{base_url}package_show"
            params = {"id": dataset['id']}
            dataset_response = requests.get(dataset_url, headers=headers, params=params)
            dataset_response.raise_for_status()
            full_datasets.append(dataset_response.json().get("result", {}))
            
        datasets_by_org[org] = full_datasets

    return datasets_by_org

def gets_one_dataset_from_datagov(dataset_name, base_url):
    dataset_url = f"{base_url}package_show"
    params = {"id": dataset_name}
    response = requests.get(dataset_url, params=params)
    response.raise_for_status()
    return response.json().get("result", {})

def compare_two_datasets(ckan_dataset, datagov_dataset):
    """
    Compare two datasets and return differences, ignoring what is always different.
    """
    differences = {}
    ignore_fields = {"id", "owner_org", "revision_id", "metadata_created", 
                    "metadata_modified", "creator_user_id"}
    ignore_org_fields = {"id", "created"}
    
    for key in ckan_dataset.keys():
        if key in ignore_fields:
            continue
            
        datagov_value = datagov_dataset.get(key)
        if ckan_dataset[key] != datagov_value:
            if key == "organization":
                # Special handling for organization field
                org_diff = {}
                for org_key in ckan_dataset[key]:
                    if org_key in ignore_org_fields:
                        continue
                    if ckan_dataset[key][org_key] != datagov_value.get(org_key):
                        org_diff[org_key] = {
                            "ckan": ckan_dataset[key][org_key],
                            "datagov": datagov_value.get(org_key)
                        }
                if org_diff:
                    differences[key] = org_diff
            else:
                differences[key] = {
                    "ckan": ckan_dataset[key],
                    "datagov": datagov_value
                }
    
    return differences


if(__name__ == "__main__"):
    datasets = get_all_datasets_from_ckan(CKAN_URL, API_KEY)
    for org, datasets in datasets.items():
        print(f"Organization: {org} ({len(datasets)} datasets)")
        for dataset in datasets:
            print(f"Dataset: {dataset['title']} ({dataset['name']})")
            """
            for argument in dataset:
                if(argument=="organization"):
                    print(f"{argument}:")
                    for key in dataset[argument]:
                        print(f"    {key}: {dataset[argument][key]}")
                elif(argument=="extras" or argument=="resources" or argument=="tags"):
                    print(f"{argument}:")
                    for key in dataset[argument]:
                        print(f"        {key}\n")
                else:
                    print(f"{argument}: {dataset[argument]}")
            
            """
            print(dataset)