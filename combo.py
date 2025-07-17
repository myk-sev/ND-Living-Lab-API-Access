from dotenv import load_dotenv
import datetime, os, requests
import pandas as pd
from hobolinkutils import get_new_token, time_formatter_hobolink

load_dotenv()

#### MANUAL SETTINGS ###
# Utilize ISO 8601 Standard YYYY-mm-ddTHH:MM:SS+HH:MM
START = "2025-01-01T00:00:00+05:00" #The time after the "+" is timezone information
END = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')

### TELLUS SETTINGS ###
KEY = os.environ.get("API_KEY")
TELLUS_API = 'https://api.tellusensors.com'
HEADERS = {'x-api-version': 'v2'}
FYE_1 = os.environ.get("DEVICE_ID_FYE1")
FYE_2 = os.environ.get("DEVICE_ID_FYE2")
Lucy_CIL = os.environ.get("DEVICE_ID_CIL")

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

if __name__ == "__main__":
    payload = {
        "loggers": LOGGER_SN,
        "start_date_time": time_formatter_hobolink(START),
        "end_date_time": time_formatter_hobolink(END)
    }
    hobolink_header = {
        'Authorization': 'Bearer ' + get_new_token(HOBOLINK_AUTH_SERVER, CLIENT_ID, CLIENT_SECRET)
    }

    hobolink_url = f"{HOBOLINK_API}/{USER_ID}"
    hobolink_response = requests.get(url=hobolink_url, headers=hobolink_header, params=payload, verify=True)
    df = pd.DataFrame.from_dict(hobolink_response.json()["observation_list"])
    df.to_csv("response.csv")
    print("CVS written")
    print(df.head())


