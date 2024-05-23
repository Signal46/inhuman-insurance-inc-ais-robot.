from msilib import Table
from robocorp.tasks import task
import requests
from RPA.JSON import JSON
from RPA.Tables import Tables

json_handler= JSON()
table_handler = Tables()

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

    print("Producer finished")

@task
def consume_traffic_data():
    """
    Inhuman Insurance, Inc. Artificial Intelligence System automation.
    Consumes traffic data work items.
    """
    print("Consumer started")


    print("Consumer finished")

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
    json_data = json_handler.load_json_from_file(filename=traffic_data_json_filepath)
    table_from_json = table_handler.create_table(json_data["value"])
    return table_handler.write_table_to_csv(table_from_json, "output/human_readable_traffic_data.csv")


