import datetime, requests, sys
from dotenv import load_dotenv
import pandas as pd
from utils import require_env

class LicorClient:
    """Client class for interacting with the LI-COR API."""
    BASE_URL = "https://api.licor.cloud/v1/data"

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def retrieve_data(self, start_time: str, end_time: str, devices: list[str]) -> pd.DataFrame:
        """Retrieve data for a specified timespan as a dataframe.

        Warning: LICOR reduces the granularity of results to fit a 100,000 record cap.
        This function will automatically split the time range and make additional API calls
        to retrieve all data, but large time ranges may take a while.
        If this poses an issue, manual adjustment of ranges is recommended.
        
        :param start_time: ISO 8601 format YYYY-MM-DDTHH:MM:SS+H:MM
        :param end_time: ISO 8601 format YYYY-MM-DDTHH:MM:SS+H:MM
        :param devices: device IDs

        :return: Pandas dataframe with timestamp, location, device nickname, data, etc
        """
        # Convert ISO 8601 format to LICOR format (YYYY-MM-DD HH:mm:SS)
        dt_start = datetime.datetime.fromisoformat(start_time)
        start_time = dt_start.strftime("%Y-%m-%d %H:%M:%S") #IS THIS ACCOUNTING FOR TIMEZONE INFO

        dt_end = datetime.datetime.fromisoformat(end_time)
        end_time = dt_end.strftime("%Y-%m-%d %H:%M:%S") #IS THIS ACCOUNTING FOR TIMEZONE INFO

        header = {
            "Authorization": f"Bearer {self.api_key}"
        }
        payload = {
            "loggers": ','.join(devices),
            "start_date_time": start_time,
            "end_date_time": end_time
        }

        print(f"\tRetrieving LICOR Data from {start_time} to {end_time}...")
        response = requests.get(url=self.BASE_URL, params=payload, headers=header)

        if response.status_code == 200:
            df = pd.DataFrame(response.json()["data"])

            if df.shape[0] == 100000:  # If result set hits the cap, recursively fetch remaining data
                print(f"\tWarning: LICOR data pull is maxed out ({df.shape[0]} records). Splitting range...")

                # Calculate midpoint
                start_dt = pd.to_datetime(start_time)
                end_dt = pd.to_datetime(end_time)
                time_diff = end_dt - start_dt

                mid_dt = start_dt + time_diff / 2
                mid_time = mid_dt.strftime('%Y-%m-%d %H:%M:%S')  # LICOR format

                # Recursively fetch split requests
                first_half = self.retrieve_data(start_time, mid_time, devices)
                second_half = self.retrieve_data(mid_time, end_time, devices)

                # Combine the results
                combined_df = pd.concat([first_half, second_half], ignore_index=True)
                print(f"\tSuccessfully combined data: {combined_df.shape[0]} total records")
                return combined_df
            else:
                print(f"\tSuccess: Retrieved {df.shape[0]} records")
                return df
        else:
            print(f"\t {response.status_code}: {response.json().get('error', 'Unknown error')} {response.json().get('error_description', '')}")
            if 'message' in response.json():
                print("\t", response.json()['message'])
            sys.exit(1)


if __name__ == "__main__":
    load_dotenv()

    ### LICOR SETTINGS ###
    LICOR_KEY = require_env("LICOR_KEY")
    IRISH_ONE = require_env("DEVICE_ID_IRISH_ONE")
    IRISH_TWO = require_env("DEVICE_ID_IRISH_TWO")
    IRISH_THREE = require_env("DEVICE_ID_IRISH_THREE")

    start_time = "2025-09-01T00:00:00+05:00"
    end_time = datetime.datetime.now(datetime.timezone.utc).isoformat()

    licor_client = LicorClient(LICOR_KEY)
    devices = [IRISH_ONE, IRISH_TWO, IRISH_THREE]

    print("Retrieving LICOR Data...")
    data = licor_client.retrieve_data(start_time, end_time, devices)
    print("Successful", "\n")

    print(data.head())
