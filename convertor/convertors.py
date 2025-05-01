import re
from typing import Dict, Any, List

def ckan_to_my_dcat(dataset: dict) -> dict:
    return dataset

"""
Because ckan has all the information we need,
 we decided to use it has a convertor basis,
 and when uploading to openmetadata basically
 we just send the information in the json format.
"""

def my_dcat_to_openmetadata(my_dcat: dict) -> dict:
    return my_dcat

"""
because openmetadata already expects a dataset from datagov
we dont need to change the format to the type
openmetadata uses
"""

def my_dcat_to_ckan(my_dcat: dict) -> dict:
    return my_dcat

"""
ckan already expects a dataset in this format
"""

def openmetadata_schema_to_ckan_org(schema):
    """
    Convert OpenMetadata schema to CKAN organization format
    """
    ckan_org = {
        'name': schema['name'].lower().replace('_', '-'),
        'title': schema.get('displayName', schema['name']).replace('_', ' ').title(),
        'description': schema.get('description', ''),
        'image_url': '',
        'extras': [
            {'key': 'source_url', 'value': schema.get('href', '')}
        ]
    }
    return ckan_org

def transform_tags(om_tags: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """Transform OpenMetadata tags to CKAN tags"""
    ckan_tags = []
    for tag in om_tags:
        if "tagFQN" in tag:
            parts = tag["tagFQN"].split('.')
            for part in parts:
                ckan_tags.append({"name": part.lower()})
    return ckan_tags
#Format: CSV\nURL: https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD
def transform_columns_to_resources(columns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform OpenMetadata columns to CKAN resources"""
    resources = []
    for column in columns:
        descriptionparts = column.get("description").split("\n")
        if len(descriptionparts)>=2:
            format = descriptionparts[0].replace("Format: ", "")
            url = descriptionparts[1].replace("URL: ", "")
        else:
            format = "Not Existent"
            url = "Not Existent"
        pattern = r"_([^_]+)$"
        correctname = re.sub(pattern, "", column.get("name", ""))
        correctname = re.sub("_", " ", correctname)
        resource = {
            "name": correctname,
            "description": "This used to be a column in openmetadata.",
            "format": format,
            "url": url,
            "datastore_active": True
        }
        
        # Add column tags if they exist
        if "tags" in column and column["tags"]:
            resource["tags"] = transform_tags(column["tags"])
        
        resources.append(resource)
    return resources

def openmetadata_to_my_dcat(dataset: dict) -> dict:
    
    my_dcat = {
        "name": dataset.get("name", "").replace("_","-"),
        "title": dataset.get("displayName", ""),
        "notes": dataset.get("description",""),
        "url": f"https://catalog.data.gov/{dataset.get("name", "").replace("_","-")}",
        "tags": transform_tags(dataset.get("tags", [])),
        "resources": transform_columns_to_resources(dataset.get("columns", [])),
        "organization": openmetadata_schema_to_ckan_org(dataset.get("databaseSchema"))
    }
    return my_dcat
