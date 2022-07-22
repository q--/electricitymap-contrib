#!/usr/bin/env python3
# coding=utf-8

import json
import logging
import re

import arrow
import requests

TIMEZONE = "Africa/Johannesburg"

PRODUCTION_URL = "https://wabi-south-africa-north-a-primary-api.analysis.windows.net/public/reports/querydata?synchronous=true"

PAYLOAD = r'{"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"q","Entity":"Station_Build_Up","Type":0}],"Select":[{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"Date_Time_Hour_Beginning"},"Name":"Query1.Date_Time_Hour_Beginning"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"Nuclear_Generation"}},"Function":0},"Name":"Sum(Station_Build_Up.Nuclear_Generation)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"International_Imports"}},"Function":0},"Name":"Sum(Station_Build_Up.International_Imports)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"Eskom_OCGT_Generation"}},"Function":0},"Name":"Sum(Station_Build_Up.Eskom_OCGT_Generation)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"Eskom_Gas_Generation"}},"Function":0},"Name":"Sum(Station_Build_Up.Eskom_Gas_Generation)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"Dispatchable_IPP_OCGT"}},"Function":0},"Name":"Sum(Station_Build_Up.Dispatchable_IPP_OCGT)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"Hydro_Water_Generation"}},"Function":0},"Name":"Sum(Station_Build_Up.Hydro_Water_Generation)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"Pumped_Water_Generation"}},"Function":0},"Name":"Sum(Station_Build_Up.Pumped_Water_Generation)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"IOS_Excl_ILS_and_MLR"}},"Function":0},"Name":"Sum(Station_Build_Up.IOS_Excl_ILS_and_MLR)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"ILS_Usage"}},"Function":0},"Name":"Sum(Station_Build_Up.ILS_Usage)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"Manual_Load_Reduction_MLR"}},"Function":0},"Name":"Sum(Station_Build_Up.Manual_Load_Reduction_MLR)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"Wind"}},"Function":0},"Name":"Sum(Station_Build_Up.Wind)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"PV"}},"Function":0},"Name":"Sum(Station_Build_Up.PV)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"CSP"}},"Function":0},"Name":"Sum(Station_Build_Up.CSP)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"Other_RE"}},"Function":0},"Name":"Sum(Station_Build_Up.Other_RE)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"Eskom_Gas_SCO_Pumping"}},"Function":0},"Name":"Sum(Station_Build_Up.Eskom_Gas_SCO_Pumping)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"Eskom_OCGT_SCO_Pumping"}},"Function":0},"Name":"Sum(Station_Build_Up.Eskom_OCGT_SCO_Pumping)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"Hydro_Water_SCO_Pumping"}},"Function":0},"Name":"Sum(Station_Build_Up.Hydro_Water_SCO_Pumping)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"Pumped_Water_SCO_Pumping"}},"Function":0},"Name":"Sum(Station_Build_Up.Pumped_Water_SCO_Pumping)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"Thermal_Gen_Excl_Pumping_and_SCO"}},"Function":0},"Name":"Sum(Station_Build_Up.Thermal_Gen_Excl_Pumping_and_SCO)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"                   Eskom_OCGT_SCO_Pumping"}},"Function":0},"Name":"Sum(Station_Build_Up.                   Eskom_OCGT_SCO_Pumping)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"                  Eskom_Gas_SCO_Pumping"}},"Function":0},"Name":"Sum(Station_Build_Up.                  Eskom_Gas_SCO_Pumping)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"                 Hydro_Water_SCO_Pumping"}},"Function":0},"Name":"Sum(Station_Build_Up.                 Hydro_Water_SCO_Pumping)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"                Pumped_Water_SCO_Pumping"}},"Function":0},"Name":"Sum(Station_Build_Up.                Pumped_Water_SCO_Pumping)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"               Thermal_Generation"}},"Function":0},"Name":"Sum(Station_Build_Up.               Thermal_Generation)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"              Nuclear_Generation"}},"Function":0},"Name":"Sum(Station_Build_Up.              Nuclear_Generation)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"             International_Imports"}},"Function":0},"Name":"Sum(Station_Build_Up.             International_Imports)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"            Eskom_OCGT_Generation"}},"Function":0},"Name":"Sum(Station_Build_Up.            Eskom_OCGT_Generation)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"           Eskom_Gas_Generation"}},"Function":0},"Name":"Sum(Station_Build_Up.           Eskom_Gas_Generation)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"          Dispatchable_IPP_OCGT"}},"Function":0},"Name":"Sum(Station_Build_Up.          Dispatchable_IPP_OCGT)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"         Hydro_Water_Generation"}},"Function":0},"Name":"Sum(Station_Build_Up.         Hydro_Water_Generation)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"        Pumped_Water_Generation"}},"Function":0},"Name":"Sum(Station_Build_Up.        Pumped_Water_Generation)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"       IOS_Excl_ILS_and_MLR"}},"Function":0},"Name":"Sum(Station_Build_Up.       IOS_Excl_ILS_and_MLR)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"      ILS_Usage"}},"Function":0},"Name":"Sum(Station_Build_Up.      ILS_Usage)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"     Manual_Load_Reduction_MLR"}},"Function":0},"Name":"Sum(Station_Build_Up.     Manual_Load_Reduction_MLR)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"    Wind"}},"Function":0},"Name":"Sum(Station_Build_Up.    Wind)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"   PV"}},"Function":0},"Name":"Sum(Station_Build_Up.   PV)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":"  CSP"}},"Function":0},"Name":"Sum(Station_Build_Up.  CSP)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q"}},"Property":" Other_RE"}},"Function":0},"Name":"Sum(Station_Build_Up. Other_RE)"}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38]}]},"DataReduction":{"DataVolume":4,"Primary":{"Sample":{}}},"SuppressedJoinPredicates":[20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38],"Version":1}}}]},"QueryId":""}],"cancelQueries":[],"modelId":165350}'

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "ActivityId": "21636bdc-6ab0-461e-b792-ec79a011a3bf",
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://app.powerbi.com",
    "Referer": "https://app.powerbi.com/",
    "RequestId": "d6fbb31e-af46-09db-7fed-900f8715c8ae",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0",
    "X-PowerBI-ResourceKey": "e4768909-4cc1-4164-a53a-4a5cab19ad76",
    "modelID":"165350"
}

def fetch_production(
    zone_key="SA",
    session=None,
    target_datetime=None,
    logger: logging.Logger = logging.getLogger(__name__),
) -> dict:
    """Requests the last known production mix (in MW) of a given country."""
    if target_datetime:
        raise NotImplementedError("This parser is not yet able to parse past dates")

    response = requests.post(PRODUCTION_URL, PAYLOAD, headers=HEADERS)
    
    data = response.json()

    #Check if data format hasn't changed
    headers = data['results'][0]['result']['data']['descriptor']['Select']
    normalized_headers = []
    for header in headers:
      normalized_headers.append(re.search(r'\.\s*([^\)]+)', header['Name']).group(1))  
    assert(normalized_headers == ['Date_Time_Hour_Beginning', 'Nuclear_Generation', 'International_Imports', 'Eskom_OCGT_Generation', 'Eskom_Gas_Generation', 'Dispatchable_IPP_OCGT', 'Hydro_Water_Generation', 'Pumped_Water_Generation', 'IOS_Excl_ILS_and_MLR', 'ILS_Usage', 'Manual_Load_Reduction_MLR', 'Wind', 'PV', 'CSP', 'Other_RE', 'Eskom_Gas_SCO_Pumping', 'Eskom_OCGT_SCO_Pumping', 'Hydro_Water_SCO_Pumping', 'Pumped_Water_SCO_Pumping', 'Thermal_Gen_Excl_Pumping_and_SCO', 'Eskom_OCGT_SCO_Pumping', 'Eskom_Gas_SCO_Pumping', 'Hydro_Water_SCO_Pumping', 'Pumped_Water_SCO_Pumping', 'Thermal_Generation', 'Nuclear_Generation', 'International_Imports', 'Eskom_OCGT_Generation', 'Eskom_Gas_Generation', 'Dispatchable_IPP_OCGT', 'Hydro_Water_Generation', 'Pumped_Water_Generation', 'IOS_Excl_ILS_and_MLR', 'ILS_Usage', 'Manual_Load_Reduction_MLR', 'Wind', 'PV', 'CSP', 'Other_RE'])

    #Work in progress, everything below copied from another parser (not working yet)

    #Parse data into correct format
    output = []

    for quarter_hourly_source_data in source_data:

        output_for_timestamp = {
            "zoneKey": zone_key,
            "datetime": arrow.get(
                quarter_hourly_source_data["DateTime"],
                "YYYY-MM-DD HH:mm:ss",
                tzinfo=TIMEZONE_NAME,
            ).datetime,
            "production": {
                "biomass": 0.0,
                "coal": 0.0,
                "gas": 0.0,
                "hydro": 0.0,
                "nuclear": 0.0,
                "oil": 0.0,
                "solar": 0.0,
                "wind": 0.0,
                "geothermal": 0.0,
                "unknown": 0.0,
            },
            "source": SOURCE_NAME,
        }

        for generation_type, outputInMW in quarter_hourly_source_data.items():
            if generation_type == "DateTime":
                continue

            if generation_type == "Coal":
                output_for_timestamp["production"]["coal"] += outputInMW
            elif generation_type == "Major Hydro" or generation_type == "SPP Minihydro":
                output_for_timestamp["production"]["hydro"] += outputInMW
            elif generation_type == "SPP Biomass":
                output_for_timestamp["production"]["biomass"] += outputInMW
            elif generation_type == "Solar":
                output_for_timestamp["production"]["solar"] += outputInMW
            elif generation_type == "Thermal-Oil":
                output_for_timestamp["production"]["oil"] += outputInMW
            elif generation_type == "Wind":
                output_for_timestamp["production"]["wind"] += outputInMW
            else:
                raise ParserException(
                    zone_key, "Unknown production type: " + generation_type
                )

        output.append(output_for_timestamp)

    return output






if __name__ == "__main__":
    """Main method, never used by the Electricity Map backend, but handy for testing."""

    print("fetch_production() ->")
    print(fetch_production())
