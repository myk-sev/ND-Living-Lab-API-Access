import os, sys, time, json, csv, itertools, datetime
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd

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
        :param string record_limit: the number of records you want to query

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
        return output

    def list_device_channels(self, device_eui: str) -> list[dict]:
        data = self._get(f"channel/list/{device_eui}")
        channels = data.get("data", [])
        if not isinstance(channels, list):
            raise RuntimeError("Unexpected response shape for channel list")
        return channels

def main():
    api_id = require_env("SENSE_CAP_ID")
    api_key = require_env("SENSE_CAP_KEY")
    client = SenseCAPClient(api_id, api_key)

    all_rows: list[dict] = []

    device_ids = client.retrieve_device_ids()

    for device_id in device_ids.keys():
        print(f"Listing channels for {device_id}...")
        channels = client.list_device_channels(device_id)
        channel_ids = []
        for ch in channels:
            # Expect fields like 'channel', 'measurementId', etc.
            ch_id = ch.get("channel") or ch.get("channelId") or ch.get("id")
            if isinstance(ch_id, int):
                channel_ids.append(ch_id)
        if not channel_ids:
            print(f"No channels for {device_id}")
            continue
        print(f"Device {device_id} has {len(channel_ids)} channels")

        for ch_id in channel_ids:
            print(f"Fetching data device={device_id} channel={ch_id} ...")
            for rec in client.iterate_all_channel_data(device_id, ch_id, per_page=200):
                all_rows.append(normalize_record(device_id, ch_id, rec))

    timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    output_path = f"sensecap_all_data_{timestamp}.csv"
    write_csv(all_rows, output_path)
    print(f"Wrote {len(all_rows)} rows to {output_path}")
    return 0


if __name__ == "__main__":
    api_id = require_env("SENSE_CAP_ID")
    api_key = require_env("SENSE_CAP_KEY")
    client = SenseCAPClient(api_id, api_key)

    devices = client.retrieve_device_ids()



