import requests
import xml.etree.ElementTree as ET
import pandas as pd
import os

# API URL for DataStructure
base_url = "http://dataservices.imf.org/REST/SDMX_XML.svc/DataStructure/IFS"

# Get the directory of the current Python script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define file paths for CSV outputs
codelist_csv = os.path.join(script_dir, "CodeLists.csv")
concepts_csv = os.path.join(script_dir, "Concepts.csv")
keyfamilies_csv = os.path.join(script_dir, "KeyFamilies.csv")

# Send GET request to the API
response = requests.get(base_url)

# Check if the response was successful
if response.status_code == 200:
    # Parse the XML response
    root = ET.fromstring(response.content)

    # Define namespaces
    namespaces = {
        '': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message',
        'structure': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure'
    }

    # Extract and process CodeLists
    codelists = []
    for codelist in root.findall(".//structure:CodeList", namespaces):
        cl_id = codelist.get("id")
        cl_name = codelist.find("structure:Name", namespaces).text if codelist.find("structure:Name", namespaces) is not None else None
        for code in codelist.findall("structure:Code", namespaces):
            code_value = code.get("value")
            code_description = code.find("structure:Description", namespaces).text if code.find("structure:Description", namespaces) is not None else None
            codelists.append({"CodeList ID": cl_id, "CodeList Name": cl_name, "Code Value": code_value, "Code Description": code_description})

    # Convert CodeLists to a DataFrame and save to CSV
    pd.DataFrame(codelists).to_csv(codelist_csv, index=False)
    print(f"CodeLists saved to {codelist_csv}")

    # Extract and process Concepts
    concepts = []
    for concept in root.findall(".//structure:Concept", namespaces):
        concept_id = concept.get("id")
        concept_name = concept.find("structure:Name", namespaces).text if concept.find("structure:Name", namespaces) is not None else None
        concept_desc = concept.find("structure:Description", namespaces).text if concept.find("structure:Description", namespaces) is not None else None
        concepts.append({"Concept ID": concept_id, "Concept Name": concept_name, "Description": concept_desc})

    # Convert Concepts to a DataFrame and save to CSV
    pd.DataFrame(concepts).to_csv(concepts_csv, index=False)
    print(f"Concepts saved to {concepts_csv}")

    # Extract and process KeyFamilies
    keyfamilies = []
    for keyfamily in root.findall(".//structure:KeyFamily", namespaces):
        kf_id = keyfamily.get("id")
        kf_name = keyfamily.find("structure:Name", namespaces).text if keyfamily.find("structure:Name", namespaces) is not None else None
        for component in keyfamily.findall(".//structure:Components/*", namespaces):
            component_type = component.tag.split('}')[-1]  # Get tag name without namespace
            concept_ref = component.get("conceptRef")
            keyfamilies.append({"KeyFamily ID": kf_id, "KeyFamily Name": kf_name, "Component Type": component_type, "Concept Ref": concept_ref})

    # Convert KeyFamilies to a DataFrame and save to CSV
    pd.DataFrame(keyfamilies).to_csv(keyfamilies_csv, index=False)
    print(f"KeyFamilies saved to {keyfamilies_csv}")

else:
    print(f"Failed to retrieve data. HTTP Status Code: {response.status_code}")
