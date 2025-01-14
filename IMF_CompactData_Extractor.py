import os
import requests
from xml.etree import ElementTree as ET
import pandas as pd

# File path for the CodeLists.csv file
script_dir = os.path.dirname(os.path.abspath(__file__))
codelists_file = os.path.join(script_dir, "CodeLists.csv")

# Load the CodeLists.csv into a dataframe
df_codelist = pd.read_csv(codelists_file)

# Filter dimensions
countries = df_codelist[df_codelist["CodeList ID"] == "CL_AREA_IFS"]["Code Value"].tolist()[:100]  # First 100 countries
indicators = df_codelist[df_codelist["CodeList ID"] == "CL_INDICATOR_IFS"]["Code Value"].tolist()

# Lookup dictionary for country names
country_lookup = dict(
    zip(
        df_codelist[df_codelist["CodeList ID"] == "CL_AREA_IFS"]["Code Value"],
        df_codelist[df_codelist["CodeList ID"] == "CL_AREA_IFS"]["Code Description"]
    )
)

# Base URL for the API
base_url = "http://dataservices.imf.org/REST/SDMX_XML.svc/CompactData/IFS"

# Start and End Periods
start_period = 2009
end_period = 2009

# Maximum indicators per API request
chunk_size = 2

# Prepare sample_data by dynamically fetching API responses
input_data = []

for country in countries:  # Loop through the first 100 countries
    for i in range(0, len(indicators), chunk_size):  # Split indicators into chunks of 22
        indicator_chunk = indicators[i:i + chunk_size]
        indicator_part = "+".join(indicator_chunk)

        # Construct the API URL
        api_url = f"{base_url}/{country}.{indicator_part}?startPeriod={start_period}&endPeriod={end_period}"
        
        # Print the API URL
        print(f"Processing API: {api_url}")
        
        # Send the GET request
        response = requests.get(api_url)

        # Check if the response is valid XML
        if response.status_code == 200 and "xml" in response.headers.get("Content-Type", "").lower():
            try:
                root = ET.fromstring(response.text)

                # Parse observations
                observations = []
                for series in root.findall(".//{http://dataservices.imf.org/compact/IFS}Series"):
                    for obs in series.findall("{http://dataservices.imf.org/compact/IFS}Obs"):
                        time_period = obs.attrib["TIME_PERIOD"]
                        value = obs.attrib.get("OBS_VALUE", "")
                        status = obs.attrib.get("OBS_STATUS", "")
                        observations.append({"time_period": time_period, "value": value, "status": status})

                # Add to sample_data
                input_data.append({
                    "country_name": country_lookup.get(country, "Unknown Country"),
                    "country_code": country,
                    "indicator_name": f"Indicators {i + 1}-{i + chunk_size}",
                    "indicator_code": indicator_part,
                    "observations": observations
                })
            except ET.ParseError:
                print(f"Failed to parse XML for API: {api_url}")
        else:
            print(f"Failed to fetch data for {country}. HTTP Status: {response.status_code}, Content-Type: {response.headers.get('Content-Type')}")
            print(f"Response: {response.text[:500]}")  # Print a snippet of the response for debugging

# Function to Transform Time Periods
def transform_time_periods(data):
    for entry in data:
        for observation in entry["observations"]:
            time_period = observation["time_period"]

            # Transform monthly periods
            if "-" in time_period and not time_period.startswith("Q"):
                year, month = time_period.split("-")
                if len(month) == 2 and month.startswith("0"):  # Handle months 01-09
                    month = month[1]
                observation["time_period"] = f"{year}M{month}"

            # Transform quarterly periods
            if "-Q" in time_period:
                observation["time_period"] = time_period.replace("-Q", "Q")

    return data

# Transform the Data
sample_data = transform_time_periods(input_data)

# Define time period range for placeholders
time_periods = set()
for year in range(start_period, end_period + 1):
    time_periods.add(str(year))  # Annual
    for quarter in range(1, 5):
        time_periods.add(f"{year}Q{quarter}")  # Quarterly
    for month in range(1, 13):
        time_periods.add(f"{year}M{month}")  # Monthly

# Sort time periods
all_periods = sorted(
    time_periods,
    key=lambda period: (
        int(period[:4]),  # Year
        0 if "Q" not in period and "M" not in period else (1 if "Q" in period else 2),  # Annual < Quarterly < Monthly
        int(period[5:]) if "M" in period else (int(period[-1]) if "Q" in period else 0),  # Quarter/Month sorting
    )
)

# Columns for the DataFrame
columns = ["Country Name", "Country Code", "Indicator Name", "Indicator Code", "Attribute"] + all_periods

# Prepare rows for the DataFrame
rows = []

for data in sample_data:
    country_name = data["country_name"]
    country_code = data["country_code"]
    indicator_name = data["indicator_name"]
    indicator_code = data["indicator_code"]

    # Create placeholders for values and statuses by granularity
    annual_values = {tp: "" for tp in all_periods if "Q" not in tp and "M" not in tp}
    quarterly_values = {tp: "" for tp in all_periods if "Q" in tp}
    monthly_values = {tp: "" for tp in all_periods if "M" in tp}
    annual_statuses = {tp: "" for tp in all_periods if "Q" not in tp and "M" not in tp}
    quarterly_statuses = {tp: "" for tp in all_periods if "Q" in tp}
    monthly_statuses = {tp: "" for tp in all_periods if "M" in tp}

    # Populate the dictionaries with observations
    for obs in data["observations"]:
        time_period = obs["time_period"]
        if time_period in annual_values:
            annual_values[time_period] = obs["value"]
            annual_statuses[time_period] = obs["status"]
        elif time_period in quarterly_values:
            quarterly_values[time_period] = obs["value"]
            quarterly_statuses[time_period] = obs["status"]
        elif time_period in monthly_values:
            monthly_values[time_period] = obs["value"]
            monthly_statuses[time_period] = obs["status"]

    # Add rows for annual values and statuses
    if any(annual_values.values()):  # Only add if there's annual data
        annual_row = [country_name, country_code, indicator_name, indicator_code, "Value"] + [
            annual_values.get(tp, "") for tp in all_periods
        ]
        rows.append(annual_row)
        annual_status_row = [country_name, country_code, indicator_name, indicator_code, "Status"] + [
            annual_statuses.get(tp, "") for tp in all_periods
        ]
        rows.append(annual_status_row)

    # Add rows for quarterly values and statuses
    if any(quarterly_values.values()):  # Only add if there's quarterly data
        quarterly_row = [country_name, country_code, indicator_name, indicator_code, "Value"] + [
            quarterly_values.get(tp, "") for tp in all_periods
        ]
        rows.append(quarterly_row)
        quarterly_status_row = [country_name, country_code, indicator_name, indicator_code, "Status"] + [
            quarterly_statuses.get(tp, "") for tp in all_periods
        ]
        rows.append(quarterly_status_row)

    # Add rows for monthly values and statuses
    if any(monthly_values.values()):  # Only add if there's monthly data
        monthly_row = [country_name, country_code, indicator_name, indicator_code, "Value"] + [
            monthly_values.get(tp, "") for tp in all_periods
        ]
        rows.append(monthly_row)
        monthly_status_row = [country_name, country_code, indicator_name, indicator_code, "Status"] + [
            monthly_statuses.get(tp, "") for tp in all_periods
        ]
        rows.append(monthly_status_row)

# Create a DataFrame from the rows
df_result = pd.DataFrame(rows, columns=columns)

# Save the DataFrame to an Excel file
output_file = "output_final.xlsx"
df_result.to_excel(output_file, index=False, sheet_name="Data")

print(f"Data has been written to {output_file}")
