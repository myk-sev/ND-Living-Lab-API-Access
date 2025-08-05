import sys

from dotenv import load_dotenv
import datetime, os, requests
import pandas as pd
from hobolinkutils import get_new_token, time_formatter_hobolink

load_dotenv()

#### MANUAL SETTINGS ###
# Utilize ISO 8601 Standard YYYY-mm-ddTHH:MM:SS+HH:MM
#START = "2025-01-01T00:00:00+05:00" #The time after the "+" is timezone information
START = "2025-08-05T00:00:00+05:00" #The time after the "+" is timezone information
END = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')
TELLUS_METRICS = ["bme280.pressure", "sunrise.co2","pms5003t.d2_5"]

### TELLUS SETTINGS ###
TELLUS_KEY = os.environ.get("API_KEY")
TELLUS_API = 'https://api.tellusensors.com'
HEADERS = {'x-api-version': 'v2'}
FYE_1 = os.environ.get("DEVICE_ID_FYE1")
FYE_2 = os.environ.get("DEVICE_ID_FYE2")
LUCY_CIL = os.environ.get("DEVICE_ID_CIL")

### HOBOLINK SETTINGS ###
LOGGER_SN = os.environ.get("LOGGER_ID")
USER_ID = os.environ.get("USER_ID")
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
HOBOLINK_AUTH_SERVER = "https://webservice.hobolink.com/ws/auth/token"
HOBOLINK_API = "https://webservice.hobolink.com/ws/data/file/JSON/user"


### TIME FORMATS ###
# HOBOLINK: YYYY-MM-DD HH:mm:SS
# TELLUS:   YYYY-MM-DDTHH:MM:SS+H:MM

device_name_map = {
    FYE_1: 'FYE_1',
    FYE_2: 'FYE_2',
    LUCY_CIL: 'Lucy_CIL'
}

def retrieve_data_hobolink(start_time, end_time):
    payload = {
        "loggers": LOGGER_SN,
        "start_date_time": time_formatter_hobolink(start_time),
        "end_date_time": time_formatter_hobolink(end_time)
    }
    header = {
        'Authorization': 'Bearer ' + get_new_token(HOBOLINK_AUTH_SERVER, CLIENT_ID, CLIENT_SECRET)
    }
    host = f"{HOBOLINK_API}/{USER_ID}"

    print("Retrieving HoboLink Data...")
    response = requests.get(url=host, headers=header, params=payload, verify=True)
    print("Successful", "\n")

    df = pd.DataFrame.from_dict(response.json()["observation_list"])
    return df

def retrieve_data_tellus(start_time, end_time):
    payload = {
        "key": TELLUS_KEY,
        "deviceId": f"{FYE_1},{FYE_2},{LUCY_CIL}",
        'start': start_time,
        'end': end_time,
        'metric': ",".join(TELLUS_METRICS)
    }
    header = {'x-api-version': 'v2'}
    host = TELLUS_API + "/data"

    print("Retrieving TELLUS Data...")
    response = requests.get(url=host, headers=header, params=payload)

    if response.status_code == 200:
        print("Success")

    elif response.status_code == 413:
        print("\t", "Requested data set too large. Please modify dates.")
        sys.exit(1)

    else:
        print("\t", "New Code")

    df = pd.DataFrame(response.json())
    return df

if __name__ == "__main__":
    hobolinkDF = retrieve_data_hobolink(START, END)
    #hobolinkDF.to_csv("response.csv")
    #print("CVS written")
    #print(hobolinkDF.head())

    tellusDF = retrieve_data_tellus(START, END)






