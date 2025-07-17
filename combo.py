from dotenv import load_dotenv
import datetime, os, requests
import pandas as pd
from hobolinkutils import get_new_token

load_dotenv()
### Currently Hardcoded #### (generalize this later)
START_STR = "2025-01-01 00:00:00"
END_STR = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

#### MANUAL SETTINGS ###
START = "2025-07-01 00:00:00"
END = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

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

    token = get_new_token(HOBOLINK_AUTH_SERVER, CLIENT_ID, CLIENT_SECRET)
    # HOBOlink url to get data from file endpoints
    payload = {
        "loggers": LOGGER_SN,
        "start_date_time": START_STR,
        "end_date_time": END_STR
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


