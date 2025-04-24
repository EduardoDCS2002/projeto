from checkingckan import get_all_datasets_from_ckan, CKAN_URL, API_KEY
from checkopenmetadata import get_table_details, search_tables, TOKEN, OPENMETADATA_URL
from magic import ckan_to_dcat, dcat_to_ckan, openmetadata_to_dcat, dcat_to_openmetadata
import json

def export_ckan_to_dcat():
    datasets = get_all_datasets_from_ckan(CKAN_URL, API_KEY)
    for org, dsets in datasets.items():
        for ds in dsets:
            dcat = ckan_to_dcat(ds)
            print(json.dumps(dcat, indent=2))

def import_dcat_to_ckan():
    filename = input("Path to the .jsonld file: ")
    with open(filename) as f:
        dcat = json.load(f)
    result = dcat_to_ckan(dcat, CKAN_URL, API_KEY)
    print("Successfully imported into CKAN:", result["name"])

def export_openmetadata_to_dcat():
    query = input("Enter the table FQN (e.g., my_service.my_db.my_schema.my_table): ")
    table = get_table_details(TOKEN, query)
    if not table:
        print("Table not found.")
        return
    dcat = openmetadata_to_dcat(table)
    print(json.dumps(dcat, indent=2))

def import_dcat_to_openmetadata():
    filename = input("Path to the .jsonld file: ")
    with open(filename) as f:
        dcat = json.load(f)
    result = dcat_to_openmetadata(dcat, OPENMETADATA_URL, TOKEN)
    print("Successfully imported into OpenMetadata:", result["name"])

def main():
    print("""
[1] Export from CKAN to DCAT (print as JSON)
[2] Import DCAT JSON into CKAN
[3] Export from OpenMetadata to DCAT (print as JSON)
[4] Import DCAT JSON into OpenMetadata
[5] Exit
""")
    option = input("Select an option: ")
    if option == "1":
        export_ckan_to_dcat()
    elif option == "2":
        import_dcat_to_ckan()
    elif option == "3":
        export_openmetadata_to_dcat()
    elif option == "4":
        import_dcat_to_openmetadata()
    elif option == "5":
        print("Goodbye!")
    else:
        print("Invalid option.")

if __name__ == "__main__":
    main()
