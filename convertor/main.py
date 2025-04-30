import upload_dataset
import om_to_ckan
import ckan_to_om
import upload_to_openmetadata

def main():
    print("""
[1] Syncronize ckan with openmetadata (import from openmetadata to ckan)
[2] Syncronize openmetadata with ckan (import from ckan to openmetadata)
[3] Update datasets to ckan from datagov
[4] Update datasets to openmetadata from datagov
[5] Exit
""")
    option = input("Select an option: ")
    if option == "1":
        om_to_ckan.om_to_ckan()
    elif option == "2":
        ckan_to_om.ckan_to_om()
    elif option == "3":
        query = input("Enter search query (or press Enter for all datasets): ").strip()
        limit = input("Enter maximum datasets to sync (default 20): ").strip()

        upload_dataset.sync_all_from_datagov(
            query=query if query else "",
            limit=int(limit) if limit.isdigit() else 20
        )
    elif option == "4":
        token = upload_to_openmetadata.get_auth_token()
        upload_to_openmetadata.process_datasets(token)
    elif option == "5":
        print("Goodbye!")
    else:
        print("Invalid option.")

if __name__ == "__main__":
    main()
