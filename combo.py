import sys
from dotenv import load_dotenv
import datetime, os, requests
import pandas as pd
from utils import get_new_token, time_formatter, require_env
import seaborn as sns
import matplotlib.pyplot as plt
from sensecap import SenseCAPClient
from tellus import TellusClient

load_dotenv()

#### MANUAL SETTINGS ###
# Utilize ISO 8601 Standard YYYY-mm-ddTHH:MM:SS+HH:MM
START_TIME = "2025-09-01T00:00:00+05:00" #The time after the "+" is timezone information
#START = "2025-08-05T00:00:00+05:00" #The time after the "+" is timezone information
END_TIME = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')
TELLUS_METRICS = ["pms5003t.temperature"]
#TELLUS_METRICS = ["bme280.pressure", "sunrise.co2","pms5003t.d2_5"]

### TELLUS SETTINGS ###
TELLUS_KEY = require_env("TELLUS_KEY")
FYE_1 = require_env("DEVICE_ID_FYE1")
FYE_2 = require_env("DEVICE_ID_FYE2")
LUCY_CIL = require_env("DEVICE_ID_CIL")

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

### SENSECAP SETTINGS ###
SENSE_CAP_USER_ID = require_env("SENSE_CAP_USER_ID")
SENSE_CAP_API_KEY = require_env("SENSE_CAP_API_KEY")
SENSE_CAP_DEVICE_ID = require_env("SENSE_CAP_DEVICE_ID")

### TIME FORMATS ###
# HOBOLINK: YYYY-MM-DD HH:mm:SS
# LICOR:    YYYY-MM-DD HH:mm:SS
# TELLUS:   YYYY-MM-DDTHH:MM:SS+H:MM
# SENSECAP: unix milleseconds

device_name_map = {
    FYE_1: 'FYE_1',
    FYE_2: 'FYE_2',
    LUCY_CIL: 'Lucy_CIL'
}

def retrieve_data_hobolink(start_time, end_time):
    """
    Recursively retrieve data from HOBOLink API, automatically handling the 100,000 record limit
    by splitting time ranges and making additional API calls.
    
    Args:
        start_time: Start time for data retrieval
        end_time: End time for data retrieval  
    """        
    payload = {
        "loggers": LOGGER_SN,
        "start_date_time": start_time,
        "end_date_time": end_time
    }
    header = {
        'Authorization': 'Bearer ' + get_new_token(HOBOLINK_AUTH_SERVER, CLIENT_ID, CLIENT_SECRET)
    }
    host = f"{HOBOLINK_API}/{USER_ID}"

    print(f"\tfrom {start_time} to {end_time}")
    response = requests.get(url=host, headers=header, params=payload, verify=True)

    if response.status_code == 200:
        data = pd.DataFrame.from_dict(response.json()["observation_list"])

        # If result set hits the cap, recursively fetch remaining data
        if data.shape[0] == 100000: 
            print(f"\t Warning: Record cap reached. Splitting range...")
            
            new_start = pd.to_datetime(data["timestamp"], utc=False, errors="coerce").max()
            new_start += datetime.timedelta(seconds=1) # Advance by one second to avoid duplicate boundary record
            new_start = new_start.strftime('%Y-%m-%d %H:%M:%S')

            remaining_data = retrieve_data_hobolink(new_start, end_time)
            data = pd.concat([data, remaining_data], ignore_index=True)


        return data
    else:
        print(f"\t {response.status_code}: {response.json()['error']} {response.json()['error_description']}")
        print("\t", response.json()['message'])
        sys.exit(1)

def retrieve_data_licor(start_time, end_time, devices):
    """
    Retrieve data for a specified timespan from the LICOR API.

    Warning: LICOR reduces the granularity of results to fit a 100,000 record cap. 
    This function will split api calls to retrieve the all data, but large time ranges may take a while.
    If this poses an issue, manual adjustment of ranges is recommended.
    
    Args:
        start_time: Start time for data retrieval
        end_time: End time for data retrieval
        devices: List of device IDs to retrieve data for
    """
    header = {
        "Authorization": f"Bearer {LICOR_KEY}"
    }
    payload = {
        "loggers": ','.join(devices),
        "start_date_time": start_time,
        "end_date_time": end_time
    }
    
    print(f"Retrieving LICOR Data from {start_time} to {end_time}...")
    response = requests.get(url=LICOR_API, params=payload, headers=header)

    if response.status_code == 200:
        df = pd.DataFrame(response.json()["data"])

        if df.shape[0] == 100000: # If result set hits the cap, recursively fetch remaining data
            print(f"\tWarning: LICOR data pull is maxed out ({df.shape[0]} records). Splitting range...")
            
           
            # Calculate midpoint
            start_dt = pd.to_datetime(start_time)
            end_dt = pd.to_datetime(end_time)
            time_diff = end_dt - start_dt

            mid_dt = start_dt + time_diff / 2
            mid_time = mid_dt.strftime('%Y-%m-%d %H:%M:%S') # format
            
            # Recursively fetch split requests
            first_half = retrieve_data_licor(start_time, mid_time, devices)
            second_half = retrieve_data_licor(mid_time, end_time, devices)
            
            # Combine the results
            combined_df = pd.concat([first_half, second_half], ignore_index=True)
            print(f"\tSuccessfully combined data: {combined_df.shape[0]} total records")
            return combined_df
        else:
            print(f"\tSuccess: Retrieved {df.shape[0]} records")
            return df
    else:
        print(f"\t {response.status_code}: {response.json()['error']} {response.json()['error_description']}" )
        print("\t", response.json()['message'])
        sys.exit(1)

def plot_temperature(data):
    #UPDATE THIS TO CONVERT TIMESTAMP TO DT_OBJ
    data["timestamp"] = data["timestamp"].apply(datetime.datetime.fromisoformat)

    # Make all temperature data in Fahrenheit
    #HOBOLINK
    data.loc[(data["sensor_measurement_type"] == "Temperature") & (data["unit"] == "°C"), "value"] *= 9/5
    data.loc[(data["sensor_measurement_type"] == "Temperature") & (data["unit"] == "°C"), "value"] += 32
    data.loc[(data["sensor_measurement_type"] == "Temperature") & (data["unit"] == "°C"), "unit"] = "°F"
    #TELLUS
    data.loc[data["sensor_measurement_type"] == "pms5003t.temperature", "value"] *= 9/5
    data.loc[data["sensor_measurement_type"] == "pms5003t.temperature", "value"] += 32
    data.loc[data["sensor_measurement_type"] == "pms5003t.temperature", "unit"] = "°F"

    temperatureData = data[data["sensor_measurement_type"].isin(["pms5003t.temperature", "Temperature"])]
    print("Begin graph generation...")
    sns.lineplot(temperatureData[:10], x="timestamp", y="value", hue="station")
    plt.show()
    print("\t", "10 item graph complete")
    sns.lineplot(temperatureData[:100], x="timestamp", y="value", hue="station")
    plt.show()
    print("\t", "100 item graph complete")
    sns.lineplot(temperatureData[:1000], x="timestamp", y="value", hue="station")
    plt.show()
    print("\t", "1000 item graph complete")
    sns.lineplot(temperatureData[:10000], x="timestamp", y="value", hue="station")
    plt.show()
    print("\t", "10000 item graph complete")
    sns.lineplot(temperatureData[:100000], x="timestamp", y="value", hue="station")
    plt.show()
    print("\t", "100000 item graph complete")
    sns.lineplot(temperatureData, x="timestamp", y="value", hue="station")
    plt.show()
    print("\t", "all item graph complete")
    print("Completed", "\n")

if __name__ == "__main__":
    # hobolink_start = "2025-01-01T00:00:00+05:00"
    # print("Retrieving HoboLink Data...")
    # hobolink_df = retrieve_data_hobolink(time_formatter(hobolink_start), time_formatter(END))
    # print("Successful", "\n")

    tellusDevices = ["B8D61ABC8E6C"]
    metrics = [
        "pms5003t.temperature",
        "bme280.temperature", 
        "sunrise.temperature"
    ]

    tellusDevices = [FYE_1, FYE_2, LUCY_CIL]
    tellus_client = TellusClient(TELLUS_KEY)
    print("TELLUS retrieval started...")
    tellus_df = tellus_client.retrieve_data(START_TIME, END_TIME, tellusDevices, metrics)
    print("Successful", "\n")

    # licorDevices = [IRISH_ONE, IRISH_TWO, IRISH_THREE]
    # licorNameMap = {IRISH_ONE:"irishOne", IRISH_TWO:"irishTwo", IRISH_THREE:"irishThree"}
    # licor_df = retrieve_data_licor(time_formatter(START), time_formatter(END), licorDevices)
    # licor_df.to_csv("licor_df.csv")