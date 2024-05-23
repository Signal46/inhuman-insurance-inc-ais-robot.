import os
from robocorp.tasks import task
from robocorp import workitems
import requests
from RPA.JSON import JSON
from RPA.Tables import Tables

json_handler= JSON()
table_handler = Tables()

# JSON data keys
COUNTRY_KEY = "SpatialDim"
YEAR_KEY = "TimeDim"
RATE_KEY = "NumericValue"
GENDER_KEY = "Dim1"

@task
def produce_traffic_data():
    """
    Inhuman Insurance, Inc. Artificial Intelligence System automation.
    Produces traffic data work items.
    """
    print("Producer started")
    traffic_data_json_filepath = "output/traffic.json"
    
    download_traffic_data(traffic_data_json_filepath)
    traffic_data = parse_traffic_data_into_table(traffic_data_json_filepath)
    filtered_and_sorted_traffic_data = filter_and_sort_traffic_data(traffic_data)
    latest_data_by_country = get_latest_data_by_country(filtered_and_sorted_traffic_data)
    payloads = create_work_item_payloads(latest_data_by_country)
    save_work_item_payloads(payloads)

    print("Producer finished")


def download_traffic_data(target_traffic_data_json_filepath):
    """
    Downloads traffic data to filepath 'target_traffic_data_json_filepath'
    """    
    print("Start downloading traffic data")
    response = requests.get("https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json")
    response.raise_for_status()
    with open(target_traffic_data_json_filepath, "wb") as f:
        f.write(response.content)
    print("Traffic data successfully downloaded")

def parse_traffic_data_into_table(traffic_data_json_filepath):
    """
    Parses the raw json file 'traffic_data_json_filepath' into a datatable and returns it
    """
    print("Parsing raw json file into table")
    json_data = json_handler.load_json_from_file(filename=traffic_data_json_filepath)    
    table_from_json = table_handler.create_table(json_data["value"])
    table_handler.write_table_to_csv(table_from_json, "output/human_readable_traffic_data.csv")
    return table_from_json
   

def filter_and_sort_traffic_data(traffic_data):
    """
    "Filtering traffic data by rate, gender and sorting on year"
    """
    print("Filtering traffic data by rate, gender and sorting on year")
    max_rate = 5.0
    both_genders = "BTSX"
    table_handler.filter_table_by_column(traffic_data, RATE_KEY , "<", max_rate )
    table_handler.filter_table_by_column(traffic_data, GENDER_KEY, "==", both_genders )
    table_handler.sort_table_by_column(traffic_data, YEAR_KEY, False)
    return traffic_data

def get_latest_data_by_country(traffic_data):
    """
    Getting latest traffic data by country
    """
    print("Getting latest traffic data by country")
    country_key = COUNTRY_KEY
    traffic_data = table_handler.group_table_by_column(traffic_data, country_key)
    latest_data_by_country = []
    for group in traffic_data:
        first_row = table_handler.pop_table_row(group)
        latest_data_by_country.append(first_row)
    return latest_data_by_country

def create_work_item_payloads(latest_data_by_country):
    """
    Converting latest data by country into work item payloads
    """
    print("Converting latest data by country into work item payloads")
    payloads = []
    for row in latest_data_by_country:
        payload = dict(
            country = row[COUNTRY_KEY],
            year = row[YEAR_KEY],
            rate = row[RATE_KEY]
        )
        payloads.append(payload)
    return payloads

def save_work_item_payloads(payloads):
    """
    Create work items of the traffic data
    """
    print("Creating work items for each row of traffic data")
    for payload in payloads:
        variables = dict(traffic_data=payload)
        workitems.outputs.create(variables)
