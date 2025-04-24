# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 16:22:30 2025

@author: Kevin
"""

import requests
import pandas as pd
import time

BEA_API_KEY = "5E258DDE-B6D4-48D8-B5E5-2B438D6A5DC0"

SECTOR_LINECODES = {
    "Agriculture, forestry, fishing and hunting": 3,
    "Farms": 4,
    "Mining": 6,
    "Utilities": 10,
    "Construction": 11,
    "Manufacturing": 12,
    "Retail trade": 35,
    "Transportation and warehousing": 36,
    "Information": 45,
    "Finance and insurance": 51,
    "Educational services": 69,
    "Health care and social assistance": 70
}

def get_bea_linecodes():
    url = "https://apps.bea.gov/api/data/"
    params = {
        "UserID": BEA_API_KEY,
        "method": "GetParameterValues",
        "datasetname": "Regional",
        "ParameterName": "LineCode",
        "TableName": "SAGDP2",
        "ResultFormat": "JSON"
    }

    response = requests.get(url, params=params)
    data = response.json()
    return pd.DataFrame(data['BEAAPI']['Results']['ParamValue'])

# Example usage
linecodes_df = get_bea_linecodes()
print(linecodes_df[['Key', 'Desc']].head())

def get_gdp_by_linecode(linecode, year="2022"):
    url = "https://apps.bea.gov/api/data/"
    params = {
        "UserID": BEA_API_KEY,
        "method": "GetData",
        "datasetname": "Regional",
        "TableName": "SAGDP2",
        "LineCode": str(linecode),
        "GeoFIPS": "STATE",
        "Year": year,
        "ResultFormat": "JSON"
    }

    response = requests.get(url, params=params)
    try:
        data = response.json()
        if "BEAAPI" in data and "Results" in data["BEAAPI"] and "Data" in data["BEAAPI"]["Results"]:
            raw = data["BEAAPI"]["Results"]["Data"]
            df = pd.DataFrame(raw)

            if "GeoFips" not in df.columns:
                print(f"[⚠️ Warning] 'GeoFips' missing in LineCode {linecode}")
                print("Returned columns:", df.columns.tolist())
                print("First few rows:", df.head())
                return pd.DataFrame()

            # ✅ Rename for consistency
            df.rename(columns={"GeoFips": "GeoFIPS"}, inplace=True)

            # ✅ Filter for numeric state codes
            df = df[df["GeoFIPS"].str.isnumeric()]

            return df

        else:
            print(f"[❌ API Error] Invalid response for LineCode {linecode}")
            print("Response:", data)
            return pd.DataFrame()

    except Exception as e:
        print(f"[❌ Exception] Error parsing response for LineCode {linecode}: {e}")
        return pd.DataFrame()



# Collect GDP by state and sector
def build_state_sector_gdp(sector_codes, year="2022"):
    all_data = []

    for sector, code in sector_codes.items():
        print(f"Fetching: {sector}")
        df = get_gdp_by_linecode(code, year)
        if df.empty:
            continue  # skip if API returned nothing

        df["Sector"] = sector
        df["GSP"] = df["DataValue"].str.replace(",", "").astype(float)
        all_data.append(df[["GeoName", "Sector", "GSP"]])
        time.sleep(0.5)  # To respect API rate limits

    return pd.concat(all_data).rename(columns={"GeoName": "State"})


# Example usage
df_state_sector_gdp = build_state_sector_gdp(SECTOR_LINECODES)
print(df_state_sector_gdp.head())