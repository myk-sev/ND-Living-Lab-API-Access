import datetime, requests, sys
from dotenv import load_dotenv
import pandas as pd
from utils import require_env


class TellusClient:
    """Client object for interacting with the Tellus API."""
    HEADER = {'x-api-version': 'v2'}
    BASE_URL = 'https://api.tellusensors.com'

    def __init__(self, api_key) -> None:
        self.api_key = api_key

    def retrieve_data(self, start_time: str, end_time: str, devices: list, metrics: list) -> pd.DataFrame:
        """Retrieve data for a specified timespan as a dataframe.

        Warning: TELLUS returns a 413 status code when the requested data set is too large.
        This function will automatically split the time range and make additional API calls
        to retrieve all data, but large time ranges may take a while.
        If this poses an issue, manual adjustment of ranges is recommended.
        
        :param start_time: ISO 8601 format YYYY-MM-DDTHH:MM:SS+H:MM
        :param end_time: ISO 8601 format YYYY-MM-DDTHH:MM:SS+H:MM
        :param devices: device IDs
        :param metrics: metrics 

        :return: Pandas dataframe with timestamp, location, device nickname, data, etc
        """
        endpoint = "data"
        host = f"{self.BASE_URL}/{endpoint}"

        payload = {
            "key": self.api_key,
            "deviceId": ','.join(devices),
            'start': start_time,
            'end': end_time,
            'metric': ",".join(metrics)
        }
        print(f"\tRetrieving TELLUS Data from {start_time} to {end_time}...")
        response = requests.get(url=host, headers=self.HEADER, params=payload)

        if response.status_code == 200:
            df = pd.DataFrame(response.json())
            print(f"\tSuccess: Retrieved {df.shape[0]} records from {start_time} to {end_time}")
            return df

        elif response.status_code == 403: print(f"Warning: {response.json()['detail']}")

        elif response.status_code == 413:
            print(f"\tWarning: TELLUS data pull is too large (413 error) for {start_time} to {end_time}. Splitting range...")
            
            # Calculate midpoint
            start_dt = pd.to_datetime(start_time)
            end_dt = pd.to_datetime(end_time)
            time_diff = end_dt - start_dt

            mid_dt = start_dt + time_diff / 2
            mid_time = mid_dt.strftime('%Y-%m-%dT%H:%M:%S%z') # ISO 8601 format
            
            # Recursively fetch split requests
            first_half = self.retrieve_data(start_time, mid_time, devices, metrics)
            second_half = self.retrieve_data(mid_time, end_time, devices, metrics)
            
            # Combine the results
            combined_df = pd.concat([first_half, second_half], ignore_index=True)
            print(f"\tSuccessfully combined data: {combined_df.shape[0]} total records")
            return combined_df
        else:
            print(f"\t {response.status_code}: {response.json()['detail']}")
            sys.exit(1)


    def retrieve_device_metrics(self, device_id: str) -> tuple(dict | str):
        """Get all the metrics available for a given device.
        
        :param device_id: The device id.

        :return: Metrics paired to their descriptions and any potential error messages.
        """
        endpoint = "schema"
        host = f"{self.BASE_URL}/{endpoint}"

        payload = {
            "key": self.api_key,
            "deviceId": device_id
        }

        response = requests.get(url=host, headers=self.HEADER, params=payload)

        if response.status_code == 200:
            data = response.json()["fields"]
            output = {field["name"]: field["description"] for field in data}
            return output, 

        elif response.status_code == 403: 
            print(f"Warning: {response.json()['detail']}")

        else:
            print(f"\t {response.status_code}: {response.json()['detail']}")
            sys.exit(1)

    def retrieve_raw_request_data(self, device_ids: list[str], endpoint: str="data", metrics: list[str]=[]) - requests.models.Response:
        """Deugging tool. Get API response without post-processing

        :param device_ids: devices to retrieve data from
        :endpoint: the api branch to interact with, unless otherwise specified "data"
        :metrics: sensors to retrieve data from

        :return: raw api output
        """
        host = f"{self.BASE_URL}/{endpoint}"
        payload = {
            "key": self.api_key,
            "deviceId": ",".join(device_ids)
        }
        if metrics != []: payload[metrics] = ",".join(metrics)

        response = requests.get(url=host, headers=self.HEADER, params=payload)
        return response

if __name__ == "__main__":
    load_dotenv()

    ### TELLUS SETTINGS ###
    TELLUS_KEY = require_env("TELLUS_KEY")
    FYE_1_ID = require_env("DEVICE_ID_FYE1")
    FYE_2_ID = require_env("DEVICE_ID_FYE2")
    LUCY_CIL_ID = require_env("DEVICE_ID_CIL")

    start_time = "2025-09-01T00:00:00+05:00" 
    end_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
    metrics = ["bme280.pressure", "sunrise.co2","pms5003t.d2_5"]

    tellus_client = TellusClient(TELLUS_KEY)
    
    print("\n", "Retrieving TELLUS Data...")
    data = tellus_client.retrieve_data(start_time, end_time, [FYE_1_ID, FYE_2_ID, LUCY_CIL_ID], metrics)
    print("Successful", "\n")
    
    print(data.head())