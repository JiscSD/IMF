import os
import requests
from xml.etree import ElementTree as ET
import pandas as pd

# File path for the CodeLists.csv file
script_dir = os.path.dirname(os.path.abspath(__file__))
codelists_file = os.path.join(script_dir, "CodeLists.csv")

# Load the CodeLists.csv into a dataframe
df_codelist = pd.read_csv(codelists_file)

# Filter for 'CL_AREA_IFS' and create a lookup dictionary
df_countries = df_codelist[df_codelist["CodeList ID"] == "CL_AREA_IFS"]
country_lookup = dict(zip(df_countries["Code Value"], df_countries["Code Description"]))

# Parameters for multiple API requests
requests_params = [
    {
        "country_code": "US",
        "indicator": "PCPI_IX",
        "frequency": "",
        "start_period": "2000",
        "end_period": "2001"
    },
    {
        "country_code": "AF",
        "indicator": "BFPAD_BP6_USD",
        "frequency": "",
        "start_period": "2008",
        "end_period": "2008"
    }
]

# Base URL for the API
base_url = "http://dataservices.imf.org/REST/SDMX_XML.svc/CompactData/IFS"

# Prepare sample_data by dynamically fetching API responses
input_data = []

for params in requests_params:
    country_code = params["country_code"]
    indicator = params["indicator"]
    start_period = params["start_period"]
    end_period = params["end_period"]

    # Lookup country name
    country_name = country_lookup.get(country_code, "Unknown Country")

    # Construct the API URL
    api_url = f"{base_url}/{params['frequency']}.{country_code}.{indicator}?startPeriod={start_period}&endPeriod={end_period}"

    # Send the GET request
    response = requests.get(api_url)

    if response.status_code == 200:
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
            "country_name": country_name,
            "country_code": country_code,
            "indicator_name": "Prices, Consumer Price Index, All items, Index",
            "indicator_code": indicator,
            "observations": observations
        })
    else:
        print(f"Failed to fetch data for {country_code}. HTTP Status: {response.status_code}")


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
start_period = 2000
end_period = 2008

# Generate all periods (Annual, Quarterly, Monthly) for the given range
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
output_file = "output1.xlsx"
df_result.to_excel(output_file, index=False, sheet_name="Data")

print(f"Data has been written to {output_file}")
