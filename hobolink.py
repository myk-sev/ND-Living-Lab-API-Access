import datetime, requests, sys
from dotenv import load_dotenv
import pandas as pd
from utils import get_new_token, time_formatter, require_env


class HoboLinkClient:
    """Client object for interacting with the HoboLINK API."""
    AUTH_SERVER = "https://webservice.hobolink.com/ws/auth/token"
    BASE_URL = "https://webservice.hobolink.com/ws/data/file/JSON/user"

    def __init__(self, client_id: str, client_secret: str, user_id: str) -> None:
        """Initialize HoboLINK client with authentication credentials.
        
        :param client_id: OAuth2 client ID provided by Onset Technical Support
        :param client_secret: OAuth2 client secret provided by Onset Technical Support  
        :param user_id: HoboLINK user ID
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_id = user_id

    def _get_auth_token(self) -> str:
        """Get OAuth2 access token for API authentication.
        
        :return str: Access token for API requests
        """
        return get_new_token(self.AUTH_SERVER, self.client_id, self.client_secret)

    def retrieve_data(self, start_time: str, end_time: str, logger_sn: str) -> pd.DataFrame:
        """Retrieve data for a specified timespan as a dataframe.

        Warning: HoboLINK returns a maximum of 100,000 records per request.
        This function will automatically split the time range and make additional API calls
        to retrieve all data, but large time ranges may take a while.
        If this poses an issue, manual adjustment of ranges is recommended.
        
        :param start_time str: ISO 8601 format YYYY-MM-DDTHH:MM:SS+H:MM
        :param end_time str: ISO 8601 format YYYY-MM-DDTHH:MM:SS+H:MM
        :param logger_sn str: Logger serial number

        :return pd.DataFrame: Pandas dataframe with timestamp, logger data, etc
        """
        # Convert ISO 8601 format to HoboLINK format (YYYY-MM-DD HH:mm:SS)
        hobolink_start = time_formatter(start_time)
        hobolink_end = time_formatter(end_time)
        
        endpoint = f"{self.BASE_URL}/{self.user_id}"
        
        payload = {
            "loggers": logger_sn,
            "start_date_time": hobolink_start,
            "end_date_time": hobolink_end
        }
        
        header = {
            'Authorization': 'Bearer ' + self._get_auth_token()
        }
        
        print(f"\tRetrieving HoboLINK Data from {hobolink_start} to {hobolink_end}...")
        response = requests.get(url=endpoint, headers=header, params=payload, verify=True)

        if response.status_code == 200:
            data = pd.DataFrame.from_dict(response.json()["observation_list"])
            print(f"\tSuccess: Retrieved {data.shape[0]} records from {hobolink_start} to {hobolink_end}")
            
            # If result set hits the cap, recursively fetch remaining data
            if data.shape[0] == 100000: 
                print(f"\tWarning: HoboLINK record cap reached (100,000 records) for {hobolink_start} to {hobolink_end}. Splitting range...")
                
                # Find the latest timestamp in the current data
                latest_timestamp = pd.to_datetime(data["timestamp"], utc=False, errors="coerce").max()
                new_start_dt = latest_timestamp + datetime.timedelta(seconds=1)  # Advance by one second to avoid duplicate boundary record
                new_start = new_start_dt.strftime('%Y-%m-%dT%H:%M:%S%z')  # Convert back to ISO 8601 format
                
                # Recursively fetch remaining data
                remaining_data = self.retrieve_data(new_start, end_time, logger_sn)
                data = pd.concat([data, remaining_data], ignore_index=True)
                print(f"\tSuccessfully combined data: {data.shape[0]} total records")
            
            return data
        else:
            error_data = response.json()
            print(f"\t{response.status_code}: {error_data.get('error', 'Unknown error')} {error_data.get('error_description', '')}")
            if 'message' in error_data:
                print(f"\t{error_data['message']}")
            sys.exit(1)

    def retrieve_multiple_loggers_data(self, start_time: str, end_time: str, logger_sns: list) -> pd.DataFrame:
        """Retrieve data from multiple loggers for a specified timespan.
        
        :param start_time str: ISO 8601 format YYYY-MM-DDTHH:MM:SS+H:MM
        :param end_time str: ISO 8601 format YYYY-MM-DDTHH:MM:SS+H:MM
        :param logger_sns list(str): List of logger serial numbers
        
        :return pd.DataFrame: Combined dataframe with data from all loggers
        """
        all_data = []
        
        for logger_sn in logger_sns:
            print(f"\tRetrieving data for logger: {logger_sn}")
            logger_data = self.retrieve_data(start_time, end_time, logger_sn)
            if not logger_data.empty:
                all_data.append(logger_data)
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            print(f"\tSuccessfully combined data from {len(logger_sns)} loggers: {combined_df.shape[0]} total records")
            return combined_df
        else:
            print("\tNo data retrieved from any logger")
            return pd.DataFrame()

    def get_logger_info(self, logger_sn: str) -> dict:
        """Get information about a specific logger.
        
        Note: This method would require additional API endpoint implementation
        based on HoboLINK API documentation. Currently returns basic info.
        
        :param logger_sn str: Logger serial number
        
        :return dict: Logger information
        """
        # This would need to be implemented based on HoboLINK API capabilities
        # For now, return basic structure
        return {
            "logger_sn": logger_sn,
            "note": "Logger info retrieval requires additional API endpoint implementation"
        }


if __name__ == "__main__":
    load_dotenv()

    ### HoboLINK SETTINGS ###
    CLIENT_ID = require_env("CLIENT_ID")
    CLIENT_SECRET = require_env("CLIENT_SECRET")
    USER_ID = require_env("USER_ID")
    LOGGER_SN = require_env("LOGGER_ID")

    start_time = "2025-01-01T00:00:00+05:00" 
    end_time = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    hobolink_client = HoboLinkClient(CLIENT_ID, CLIENT_SECRET, USER_ID)
    
    print("Retrieving HoboLINK Data...")
    data = hobolink_client.retrieve_data(start_time, end_time, LOGGER_SN)
    print("Successful", "\n")
    
    print(data.head())
