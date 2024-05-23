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
def consume_traffic_data():
    """
    Inhuman Insurance, Inc. Artificial Intelligence System automation.
    Consumes traffic data work items.
    """
    print("Consumer started")
    
    for item in workitems.inputs:
        print("Retrieved workitem: "+item.id)
        traffic_data=item.payload["traffic_data"]
        if len(traffic_data["country"]) == 3:
            status, return_json = post_traffic_data_to_sales_system(traffic_data)
            if status == 200:
                print("Workitem successfully completed")
                item.done()
            else:
                print("Workitem failed with message: " + return_json["message"])
                item.fail(
                    exception_type= "APPLICATION",
                    code="TRAFFIC_DATA_POST_FAILED",
                    message=return_json["message"]
                )
        else:
            item.fail(
                exception_type="BUSINESS",
                code="INVALID_TRAFFIC_DATA",
                message=item.payload,
            )

    print("Consumer finished")
    
def post_traffic_data_to_sales_system(traffic_data):
    url = "https://robocorp.com/inhuman-insurance-inc/sales-system-api"
    response = requests.post(url,json=traffic_data)
    return response.status_code, response.json()