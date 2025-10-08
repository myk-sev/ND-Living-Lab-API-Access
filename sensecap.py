import datetime, requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from utils import require_env

class SenseCAPClient:
    BASE_URL = "https://sensecap.seeed.cc/openapi"

    def __init__(self, api_id: str, api_key: str):
        self.auth = HTTPBasicAuth(api_id, api_key)

    def _get(self, endpoint: str, params: dict | None = {}) -> dict:
        url = f"{self.BASE_URL}/{endpoint}"
        response = requests.get(url, auth=self.auth, params=params)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and str(payload.get("code")) != "0":
            raise RuntimeError(f"SenseCAP API error: {payload.get('msg')} ({payload.get('code')})")

        if payload.get("data") == []:
            print("Warning: Empty data set retrieved.")
        return payload

    def retrieve_device_ids(self) -> dict[str, str]:
        end_point = "device/list_euis"

        response = self._get(end_point)
        devices = {
            "gateways": response.get("data", {}).get("gateway", []) , #get is used to provide a default value incase none is retrieved
            "nodes": response.get("data", {}).get("node", []) 
        }

        if devices["gateways"] == [] or devices["nodes"] == []:
            print("Warning: Device retrieval not successful.")
            print("Gateways:", devices["gateways"])
            print("Nodes:", devices["nodes"])

        return devices

    def latest_data_point(self, device_id, channel_index="", measurement_id=""):
        endpoint = "view_latest_telemetry_data"

        payload = {
            "device_eui": device_id,
        }
        if channel_index != "": payload["channel_index"] = channel_index
        if measurement_id != "": payload["measurement_id"] = measurement_id

        sensecap_response = self._get(endpoint=endpoint, params=payload)["data"]

        def construct_df(channel_data):
            df = pd.DataFrame(channel_data["points"])
            df["channel_index"] = channel_data["channel_index"]
            return df

        df = pd.concat([construct_df(channel_data) for channel_data in sensecap_response], ignore_index=True) #handles multi channel devices
        return df

    def get_historic_data(self, device_id, time_start="", time_end="", channel_index="", sensor_id="", record_limit=0):
        """Can retrieve data up to 3 months old. Retrieves up to a maximum of one month at a time.

        :param string device_id: device extended unique identifier
        :param string time_start: iso 8061 time
        :param string time_end: iso 8061 time
        :param string channel_index: channel to query data from
        :param string sensor_id: sensor ID
        :param int record_limit: the number of records you want to query

        :return dataframe: 
        """
        endpoint = "list_telemetry_data"

        payload = {
            "device_eui": device_id,
        }

        if channel_index != "": payload["channel_index"] = channel_index
        if sensor_id != "": payload["measurement_id"] = sensor_id
        if time_start != "": payload["time_start"] = datetime.datetime.fromisoformat(time_start).timestamp() * 1000 #if not specified the default is one day ago
        if time_end != "": payload["time_end"] = datetime.datetime.fromisoformat(time_end).timestamp() * 1000 #if not specified the default is now
        if record_limit != 0: payload["record_limit"] = record_limit

        sensecap_response = self._get(endpoint=endpoint, params=payload)["data"]["list"]

        # the structure of the response is two groups of data
        # one is sensor info, the other is readings
        # each of these is further grouped by the source sensor
        converted_data = []
        sensor_info_set = sensecap_response[0]
        data_set = sensecap_response[1]
        for sensor_info, data in zip(sensor_info_set, data_set): #the structure of the response is 
            df = pd.DataFrame([{"measurement":entry[0], "timestamp": entry[1]} for entry in data])
            df["channel_index"] = sensor_info[0]
            df["measurement_id"] = sensor_info[1]
            converted_data.append(df)
        output = pd.concat(converted_data, ignore_index=True) #the code above flattens this into a single dataframe
        output = output[["timestamp", "channel_index", "measurement_id", "measurement"]]
        return output

    def get_aggregate_data(self, device_id, time_start="", time_end="", channel_index="", sensor_id="", interval=0):
        """Can retrieve data up to 1 year old. Default interval is 60 mins.

        :param string device_id: device extended unique identifier
        :param string time_start: iso 8061 time YYYY-MM-DDTHH:MM:SS+H:MM
        :param string time_end: iso 8061 time YYYY-MM-DDTHH:MM:SS+H:MM
        :param string channel_index: channel to query data from
        :param string sensor_id: sensor ID
        :param int interval: the length of the time period to get, unit minute.

        :return dataframe: aggregate data
        """
        endpoint = "aggregate_chart_points"

        payload = {
            "device_eui": device_id,
        }

        if time_start != "": payload["time_start"] = int(datetime.datetime.fromisoformat(time_start).timestamp() * 1000) #if not specified the default is one day ago
        if time_end != "": payload["time_end"] = int(datetime.datetime.fromisoformat(time_end).timestamp() * 1000) #if not specified the default is now
        if channel_index != "": payload["channel_index"] = channel_index
        if sensor_id != "": payload["measurement_id"] = sensor_id
        if interval != 0: payload["interval"] = interval #default is 60 minutes

        sensecap_response = self._get(endpoint=endpoint, params=payload)["data"]

        def construct_df(channel_data):
            sensor_data = channel_data["lists"]
            df = pd.DataFrame(sensor_data)
            df["channel"] = channel_data["channel"]
            return df

        df = pd.concat([construct_df(channel_data) for channel_data in sensecap_response], ignore_index=True)
        df = df[["time", "channel", "measurement_id", "average_value"]]
        return df

    def list_device_channels(self, device_eui: str) -> list[dict]:
        data = self._get(f"channel/list/{device_eui}")
        channels = data.get("data", [])
        if not isinstance(channels, list):
            raise RuntimeError("Unexpected response shape for channel list")
        return channels

    def retrieve_raw_request_data(self, endpoint: str, payload: dict | None = None) -> requests.models.Response:
        """Debugging tool. Return raw API response without post-processing.

        :param endpoint: API endpoint path relative to base (e.g., "view_latest_telemetry_data")
        :param params: Query parameters to include in the request

        :return: Raw requests Response
        """
        url = f"{self.BASE_URL}/{endpoint}"
        response = requests.get(url, auth=self.auth, params=payload or {})
        return response

def plot_data(data, title, y_col, time_col = "timestamp"):
    import seaborn as sns
    import matplotlib.pyplot as plt

    data[time_col] = pd.to_datetime(data[time_col])

    plt.xticks(rotation=45)
    plt.title(title)
    sns.lineplot(data, x=time_col, y=y_col)
    plt.show()

if __name__ == "__main__":
    api_id = require_env("SENSE_CAP_USER_ID")
    api_key = require_env("SENSE_CAP_API_KEY")
    client = SenseCAPClient(api_id, api_key)
    devices = client.retrieve_device_ids()

    start = "2025-10-01T00:00:00"
    device_id = "2CF7F1C0708000D7"

    #data = client.get_historic_data(device_id, time_start=start)
    #print(data)
