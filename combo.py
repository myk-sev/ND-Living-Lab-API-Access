import datetime, os, requests, sys

from dotenv import load_dotenv
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from hobolink import HoboLinkClient
from licor import LicorClient
from sensecap import SenseCAPClient
from tellus import TellusClient
from utils import require_env

load_dotenv()

#### MANUAL SETTINGS ###
# Utilize ISO 8601 Standard YYYY-mm-ddTHH:MM:SS+HH:MM
START_TIME = "2025-09-01T00:00:00+05:00" #The time after the "+" is timezone information
END_TIME = datetime.datetime.now(datetime.timezone.utc).isoformat()
TELLUS_METRICS = ["bme280.pressure", "sunrise.co2","pms5003t.d2_5"]

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
    hobolink_client = HoboLinkClient(CLIENT_ID, CLIENT_SECRET, USER_ID)
    
    print("Retrieving HoboLINK Data...")
    data = hobolink_client.retrieve_data(START_TIME, END_TIME, LOGGER_SN)
    print("Successful", "\n")

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

    licorDevices = [IRISH_ONE, IRISH_TWO, IRISH_THREE]
    licorNameMap = {IRISH_ONE:"irishOne", IRISH_TWO:"irishTwo", IRISH_THREE:"irishThree"}
    licor_client = LicorClient(LICOR_KEY)
    print("LICOR data retrieval started...")
    licor_df = licor_client.retrieve_data(START_TIME, END_TIME, licorDevices)
    print("Successful", "\n")