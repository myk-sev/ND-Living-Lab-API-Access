import datetime, json, os, requests, sys, urllib3
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

def time_formatter(dt_str):
    """Ensures time entry is compatible with HOBOlink & LICOR APIs: YYYY-MM-DD HH:mm:SS"""
    dt_obj = datetime.datetime.fromisoformat(dt_str)
    hobolink_time = dt_obj.strftime("%Y-%m-%d %H:%M:%S") #IS THIS ACCOUNTING FOR TIMEZONE INFO
    return hobolink_time

def retrieve_tellus_metrics(api_key, device_id):
    """Gets all the metrics available for a given device.
    
    :api_key str: The API key for the Tellus network.
    :device_id str: The device id.

    :return dict(str, str): Metrics paired to their descriptions.
    """
    host = 'https://api.tellusensors.com' + "/schema"

    header = {'x-api-version': 'v2'}
    payload = {
        "key": api_key,
        "deviceId": device_id
    }

    response = requests.get(url=host, headers=header, params=payload)
    data = response.json()["fields"]

    output = {field["name"]: field["description"] for field in data}

    return output

def require_env(var_name):
    value = os.environ.get(var_name)
    if not value:
        raise RuntimeError(f"Environment variable {var_name} is required")
    return value