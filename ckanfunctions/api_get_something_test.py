#!/usr/bin/env python
import requests
import json
import pprint

# Define the URL and parameters (if any)
url = 'http://demo.ckan.org/api/3/action/group_list'
params = {}  # Add any parameters if needed

# Make the HTTP GET request using the requests library
response = requests.get(url, params=params)

# Check if the request was successful (status code 200)
assert response.status_code == 200, f"Request failed with status code {response.status_code}"

# Use the json module to load CKAN's response into a dictionary
response_dict = response.json()

# Check the contents of the response
assert response_dict['success'] is True, "Response was not successful"
result = response_dict['result']

# Pretty-print the result
# podia imprimir apenos o result e obtia só os valores "result", ou posso imprimir o json completo.
pprint.pprint(response_dict)