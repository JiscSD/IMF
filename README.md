# IMF Data Extraction Framework

This repository contains scripts for automating data extraction from the IMF's IFS dataset using the SDMX 2.0 API. The framework is designed to construct and execute API calls for retrieving data from IMF datasets while handling dataset constraints and API limitations.

## Scripts Overview

1. **`IMF_DataStructure_Extractor.py`**: Extracts CodeLists and parameters (countries, indicators, frequencies, etc.) required for constructing API requests.
2. **`IMF_CompactData_Extractor_w_sample.py`**: Demonstrates API construction and validates sample requests with predefined countries and indicators.
3. **`IMF_CompactData_Extractor.py`**: Automates the generation of API calls by looping through combinations of countries, frequencies, and indicators to fetch data.  
   *Note: This script is incomplete due to API constraints (3000 records per request) and performance challenges for large datasets.*

## Notes

- This framework is **incomplete** and serves as a Initial skeleton.
- For large datasets, it is recommended to use manual bulk download options from the IMF website for efficiency.
- The scripts are built around the SDMX 2.0 XML API, which is stable but has limitations for large-scale automation.

## Documentation

For detailed documentation on the process, scripts, and limitations, refer to the [Confluence Page](https://jiscdev.atlassian.net/wiki/x/B4A-TAE).

---

### Disclaimer

This codebase is a skeleton for further development and primarily intended for reference. Contributions and enhancements are welcome.
