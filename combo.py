from dotenv import load_dotenv, find_dotenv
import datetime, os, requests
import pandas as pd
from HOBOlink_parse import get_new_token

load_dotenv()
### Currently Hardcoded #### (generalize this later)
START_STR = "2025-01-01 00:00:00"
END_STR = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

### TELLUS SETTINGS ###
KEY = os.environ.get("API_KEY")
HOST = 'https://api.tellusensors.com'
HEADERS = {'x-api-version': 'v2'}
FYE_1 = os.environ.get("DEVICE_ID_FYE1")
FYE_2 = os.environ.get("DEVICE_ID_FYE2")
Lucy_CIL = os.environ.get("DEVICE_ID_CIL")

### HOBOLINK SETTINGS ###
LOGGER_SN = os.environ.get("LOGGER_ID")
USER_ID = os.environ.get()

#### MANUAL SETTINGS ###
START = "2025-07-01 00:00:00"
END = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

### TIME FORMATS ###
# HOBOLINK: YYYY-MM-DD HH:mm:SS
# TELLUS:   YYYY-MM-DDTHH:MM:SS+H:MM

if __name__ == "__main__":
    # HOBOlink account and device info
    user_id = os.environ.get("USER_ID")  # user ID found on HOBOlink
    # credentials provided by Onset Tech support
    client_id = os.environ.get("CLIENT_ID")
    client_secret = os.environ.get("CLIENT_SECRET")

    # HOBOlink authentication server
    # url provided by HOBOlink Web Services V3 Developer's Guide
    auth_server_url = "https://webservice.hobolink.com/ws/auth/token"
    token = get_new_token(auth_server_url, client_id, client_secret)
    print("success")
    # HOBOlink url to get data from file endpoints
    payload = {
        "loggers": LOGGER_SN,
        "start_date_time": START_STR,
        "end_date_time": END_STR
    }
    hobolink_header = {
        'Authorization': 'Bearer ' + get_new_token(auth_server_url, client_id, client_secret)
    }

    hobolink_url = f"https://webservice.hobolink.com/ws/data/file/JSON/user/{user_id}"
    hobolink_response = requests.get(url=hobolink_url, headers=hobolink_header, params=payload, verify=True)
    df = pd.DataFrame.from_dict(hobolink_response.json()["observation_list"])
    df.to_csv("response.csv")
    print("CVS written")