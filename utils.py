import json, os, requests, sys, urllib3
import pandas as pd
urllib3.disable_warnings()  # Warnings occur each time a token is generated.

def get_new_token(auth_server_url, client_id, client_secret):
    """Obtain a new OAuth 2.0 token from the authentication server."""
    
    token_req_payload = {'grant_type': 'client_credentials'}

    #print("Retrieving New Token...")
    token_response = requests.post(auth_server_url,
                                   data=token_req_payload,
                                   verify=False,
                                   allow_redirects=False,
                                   auth=(client_id, client_secret)
                                   )

    if token_response.status_code != 200:
        print("Failed to obtain token from the OAuth 2.0 server")
        sys.exit(1)
    else:
        pass
        #print("Successful", "\n")

    tokens = json.loads(token_response.text)
    return tokens['access_token']

def require_env(var_name):
    value = os.environ.get(var_name)
    if not value:
        raise RuntimeError(f"Environment variable {var_name} is required")
    return value

def extract_time_period(data: pd.DataFrame, start_time: str, end_time: str, time_col: str="timestamp") -> pd.DataFrame:
    """Remove data from dataset that falls outside a specified daily period.

    :param data: TellusClient output
    :param start_time: start of the desired time period. format: HH:MM:SS
    :param end_time: end of the desired time period. format: HH:MM:SS
    """
    # generated relevant data
    start_tobj = pd.to_datetime(start_time).time()
    end_tobj = pd.to_datetime(end_time).time()
    data_times = data[time_col].dt.time

    # create mask
    mask = (data_times >= start_tobj) > (data_times <= end_tobj)

    # apply mask
    return data[mask]