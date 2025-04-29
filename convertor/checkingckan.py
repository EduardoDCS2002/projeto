import requests
CKAN_URL = "http://localhost:5000/api/3/action/"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJtSDF4V2pmQkEtMXRJZXNMRGZTaEk4U0dPX0JfdEtaVi1jSHBIeVJfcTJRIiwiaWF0IjoxNzQ0Mjk4MTIyfQ.7yBFMAk28YDFdq39PHnHPPJDaQxtRWxmKsiU0p4x6cc"
DATA_GOV_API = "https://catalog.data.gov/api/3/action/"
def get_all_datasets_from_ckan(base_url, api_key):
    """
    Only needs your ckan url and your api key.
    Returns a dictionary where keys are organization names and values are lists of datasets.
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
        org_datasets_url = f"{base_url}organization_show"
        params = {"id": org, "include_datasets": True}
        org_response = requests.get(org_datasets_url, headers=headers, params=params)
        org_response.raise_for_status()
        org_data = org_response.json().get("result", {})
        datasets = org_data.get("packages", [])
        datasets_by_org[org] = datasets

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

datasets = get_all_datasets_from_ckan(CKAN_URL, API_KEY)
for org, datasets in datasets.items():
    print(f"Organization: {org}")
    for dataset in datasets:
        print(f"  Dataset:\n {dataset}")
        """dataset_info = gets_one_dataset_from_datagov(dataset["name"], DATA_GOV_API)
        if dataset_info:
            differences = compare_two_datasets(dataset, dataset_info)
            if differences:
                print(f"    Differences: {differences}")
            else:
                print("    No differences found.")
        else:
            print("    Dataset not found in data.gov.")
        """