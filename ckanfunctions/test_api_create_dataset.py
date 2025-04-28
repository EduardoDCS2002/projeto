#!/usr/bin/env python
import requests
import json
import pprint

# Put the details of the dataset we are going to create into a dict.
dataset_dict = {
    'name': 'api_created_dataset',
    'notes': 'Test runs with the api',
    'owner_org': 'testedeorganizacao'
}

# Define the API endpoint to create a dataset
url = 'http://localhost:5000/api/action/package_create'


headers = {
    'Authorization': "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJqNnFrQ3dhcVotM1dVLXEyYm9WbDNfRjQ1Z2lBVXBFT3BvRmlrYVFudFEwIiwiaWF0IjoxNzQzNTMyNTk1fQ.kp_u89vzmpXrMqJEDTQc6a3xIkbW7lhGTP5C91uyCEg",
    'Content-Type': 'application/json'
}

# Make the HTTP POST request using the requests library
response = requests.post(url, headers=headers, data=json.dumps(dataset_dict))

# Check if the request was successful (status code 200)
assert response.status_code == 200, f"Request failed with status code {response.status_code}"

# Use the json module to load CKAN response into a dictionary
response_dict = response.json()

# Check the contents of the response
assert response_dict['success'] is True, "Response was not successful"

# package_create returns the created package as its result
created_package = response_dict['result']
pprint.pprint(created_package)