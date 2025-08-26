import sys

from dotenv import load_dotenv
import datetime, os, requests
import pandas as pd
from hobolinkutils import get_new_token, time_formatter_hobolink
import seaborn as sns
import matplotlib.pyplot as plt

load_dotenv()

#### MANUAL SETTINGS ###
# Utilize ISO 8601 Standard YYYY-mm-ddTHH:MM:SS+HH:MM
START = "2025-01-01T00:00:00+05:00" #The time after the "+" is timezone information
#START = "2025-08-05T00:00:00+05:00" #The time after the "+" is timezone information
END = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')
TELLUS_METRICS = ["pms5003t.temperature"]
#TELLUS_METRICS = ["bme280.pressure", "sunrise.co2","pms5003t.d2_5"]

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
    """
    Retrieve data for a specified timespan from the LICOR API.

    Warning: LICOR auto reduces the granularity of results to fit a 100,000 record cap. 
    This script can split api calls to retrieve the maximum amount of data, but this process is slow.
    In these cases manual adjustment of ranges is possible.
    
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


def generate_temperature_graph(data):
    hobolink_start = "2025-01-01T00:00:00+05:00"
    hobolinkDF = retrieve_data_hobolink(time_formatter_hobolink(hobolink_start), time_formatter_hobolink(END))
    #print(hobolinkDF)
    #hobolinkDF.to_csv("response.csv")
    #print("CVS written")
    #print(hobolinkDF.head())

    tellusDevices = [FYE_1, FYE_2, LUCY_CIL]
    tellus_start = "2025-08-01T00:00:00+05:00"
    tellusDF = retrieve_data_tellus(tellus_start, END, tellusDevices, TELLUS_METRICS)
    #print(tellusDF)

    licorDevices = [IRISH_ONE, IRISH_TWO, IRISH_THREE]
    licorNameMap = {IRISH_ONE:"irishOne", IRISH_TWO:"irishTwo", IRISH_THREE:"irishThree"}
    licorDF = retrieve_data_licor(time_formatter_hobolink(START), time_formatter_hobolink(END), licorDevices)

    #print(licorDF)

    #licorDF.to_csv("licor.csv")
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

    ### Add in Tellus Data ###

    tellusSplit = {}
    for metric in TELLUS_METRICS:
        tellusColumns = [
            "timestamp",
            "nickname",
            metric
        ]
        tellusSplit[metric] = tellusDF[tellusColumns]
        tellusSplit[metric].rename(columns={metric:"value"}, inplace=True)
        tellusSplit[metric]["sensor_measurement_type"] = metric
    tellusRecombined = pd.concat([tellusSplit[metric] for metric in tellusSplit], ignore_index=True)
    tellusRecombined.rename(columns={"nickname": "station"}, inplace=True)

    allData = pd.concat([hobolinkLicorMerge, tellusRecombined], ignore_index=True)
    allData.to_csv("merge3.csv")

    plot_temperature(allData)

if __name__ == "__main__":
    # hobolink_start = "2025-01-01T00:00:00+05:00"
    # print("Retrieving HoboLink Data...")
    # hobolink_df = retrieve_data_hobolink(time_formatter_hobolink(hobolink_start), time_formatter_hobolink(END))
    # print("Successful", "\n")


    # tellusDevices = [FYE_1, FYE_2, LUCY_CIL]
    # tellus_start = "2025-08-01T00:00:00+05:00"
    # tellus_df = retrieve_data_tellus(tellus_start, END, tellusDevices, TELLUS_METRICS)
    # print(tellus_df)
    # print(tellus_df.columns)

    licorDevices = [IRISH_ONE, IRISH_TWO, IRISH_THREE]
    licorNameMap = {IRISH_ONE:"irishOne", IRISH_TWO:"irishTwo", IRISH_THREE:"irishThree"}
    licor_df = retrieve_data_licor(time_formatter_hobolink(START), time_formatter_hobolink(END), licorDevices)
    licor_df.to_csv("licor_df.csv")