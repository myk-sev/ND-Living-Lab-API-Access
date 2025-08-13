import sys

from dotenv import load_dotenv
import datetime, os, requests
import pandas as pd
from hobolinkutils import get_new_token, time_formatter_hobolink

load_dotenv()

#### MANUAL SETTINGS ###
# Utilize ISO 8601 Standard YYYY-mm-ddTHH:MM:SS+HH:MM
START = "2025-01-01T00:00:00+05:00" #The time after the "+" is timezone information
#START = "2025-08-05T00:00:00+05:00" #The time after the "+" is timezone information
END = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')
TELLUS_METRICS = ["bme280.pressure", "sunrise.co2","pms5003t.d2_5"]

### TELLUS SETTINGS ###
TELLUS_KEY = os.environ.get("TELLUS_KEY")
TELLUS_API = 'https://api.tellusensors.com'
FYE_1 = os.environ.get("DEVICE_ID_FYE1")
FYE_2 = os.environ.get("DEVICE_ID_FYE2")
LUCY_CIL = os.environ.get("DEVICE_ID_CIL")

### HOBOLINK SETTINGS ###
HOBOLINK_AUTH_SERVER = "https://webservice.hobolink.com/ws/auth/token"
HOBOLINK_API = "https://webservice.hobolink.com/ws/data/file/JSON/user"
LOGGER_SN = os.environ.get("LOGGER_ID")
USER_ID = os.environ.get("USER_ID")
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")


### LICOR SETTINGS ###
LICOR_API = "https://api.licor.cloud/v1/data"
LICOR_KEY = os.environ.get("LICOR_KEY")
IRISH_ONE = os.environ.get("DEVICE_ID_IRISH_ONE")
IRISH_TWO = os.environ.get("DEVICE_ID_IRISH_TWO")
IRISH_THREE = os.environ.get("DEVICE_ID_IRISH_THREE")



### TIME FORMATS ###
# HOBOLINK: YYYY-MM-DD HH:mm:SS
# LICOR:    YYYY-MM-DD HH:mm:SS
# TELLUS:   YYYY-MM-DDTHH:MM:SS+H:MM

device_name_map = {
    FYE_1: 'FYE_1',
    FYE_2: 'FYE_2',
    LUCY_CIL: 'Lucy_CIL'
}

def retrieve_data_hobolink(start_time, end_time):
    payload = {
        "loggers": LOGGER_SN,
        "start_date_time": start_time,
        "end_date_time": end_time
    }
    header = {
        'Authorization': 'Bearer ' + get_new_token(HOBOLINK_AUTH_SERVER, CLIENT_ID, CLIENT_SECRET)
    }
    host = f"{HOBOLINK_API}/{USER_ID}"

    print("Retrieving HoboLink Data...")
    response = requests.get(url=host, headers=header, params=payload, verify=True)

    if response.status_code == 200:
        df = pd.DataFrame.from_dict(response.json()["observation_list"])

        if df.shape[0] == 100000:
            print("\t", "Warning: HOBOLink data pull is maxed out. Please choose smaller settings.")

        print("Successful", "\n")
        return df
    else:
        print(f"\t {response.status_code}: {response.json()['error']} {response.json()['error_description']}")
        print("\t", response.json()['message'])
        sys.exit(1)

def retrieve_data_tellus(start_time, end_time, devices, metrics):
    header = {'x-api-version': 'v2'}
    host = TELLUS_API + "/data"
    payload = {
        "key": TELLUS_KEY,
        "deviceId": ','.join(devices),
        'start': start_time,
        'end': end_time,
        'metric': ",".join(metrics)
    }

    print("Retrieving TELLUS Data...")
    response = requests.get(url=host, headers=header, params=payload)

    if response.status_code == 200:
        df = pd.DataFrame(response.json())
        print("Success", "\n")
        return df
    elif response.status_code == 413:
        print("\t", "Requested data set too large. Please modify dates.")
        sys.exit(1)
    else:
        print(f"\t {response.status_code}: {response.json()['detail']}")
        sys.exit(1)


def retrieve_data_licor(start_time, end_time, devices):
    header = {
        "Authorization": f"Bearer {LICOR_KEY}"
    }
    payload = {
        "loggers": ','.join(devices),
        "start_date_time": start_time,
        "end_date_time": end_time
    }
    print("Retrieving LICOR Data...")
    response = requests.get(url=LICOR_API, params=payload, headers=header)

    if response.status_code == 200:
        df = pd.DataFrame(response.json()["data"])

        if df.shape[0] == 100000:
            print("\t", "Warning: LICOR data pull is maxed out. Please choose smaller settings.")

        print("Success", "\n")
        return df
    else:
        print(f"\t {response.status_code}: {response.json()['error']} {response.json()['error_description']}" )
        print("\t", response.json()['message'])
        sys.exit(1)


if __name__ == "__main__":
    hobolinkDF = retrieve_data_hobolink(time_formatter_hobolink(START), time_formatter_hobolink(END))
    print(hobolinkDF)
    #hobolinkDF.to_csv("response.csv")
    #print("CVS written")
    #print(hobolinkDF.head())

    #tellusDevices = [FYE_1, FYE_2, LUCY_CIL]
    #tellusDF = retrieve_data_tellus(START, END, tellusDevices, TELLUS_METRICS)
    #print(tellusDF)

    licorDevices = [IRISH_ONE, IRISH_TWO, IRISH_THREE]
    licorNameMap = {IRISH_ONE:"irishOne", IRISH_TWO:"irishTwo", IRISH_THREE:"irishThree"}
    licorDF = retrieve_data_licor(time_formatter_hobolink(START), time_formatter_hobolink(END), licorDevices)

    #print(licorDF)

    licorDF.to_csv("licor.csv")
    #hobolinkDF.to_csv("hobolink.csv")

    ### Combine hobolink & licor data
    licorDF["station"] = licorDF["logger_sn"].apply(lambda entry: licorNameMap[entry])
    licorKeyData = licorDF[[
        "timestamp",
        "station",
        "sensor_sn",
        "data_type",
        "value",
        "unit",
        "sensor_measurement_type"
    ]]

    hobolinkDF["station"] = "HOBOLink"
    hobolinkKeyData = hobolinkDF[[
        "timestamp",
        "station",
        "sensor_sn",
        "si_value",
        "si_unit",
        "sensor_measurement_type"
    ]]

    licorKeyData.rename(columns={
        "sensor_sn": "sensor",
    }, inplace=True)

    hobolinkKeyData.rename(columns={
        "sensor_sn": "sensor",
        "si_value": "value",
        "si_unit": "unit",
    }, inplace=True)

    hobolinkLicorMerge = pd.concat([licorKeyData, hobolinkKeyData], ignore_index=True)
    hobolinkLicorMerge.to_csv("merge.csv")









