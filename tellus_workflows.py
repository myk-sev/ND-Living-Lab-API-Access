import pandas as pd
from tellus import TellusClient
from utils import extract_time_period, validate_date
import datetime

def generate_night_temperature_averages(client: TellusClient, device_ids: list[str], start_day: str, end_day: str, metrics: list[str]=["sunrise.temperature"], time_zone_delta: int=-5):
    """Calculate the average temperature from 2am-4am for each day and device provided.

    :param client: instantiated TellusClient object
    :param device_ids: devices to retrieve data from
    :param start_day: format YYYY-MM-DD
    :param end_day: format YYYY-MM-DD
    :param metrics: sensors to retrieve data from. sunrise is the default.
    :param time_zone_delta: hour offset for the target timezone

    :return nightly_averages: the average temperature from 2am-4am for each day and device provided 
    """
    data = retrieve_data_between_days(client, device_ids=device_ids, start_day=start_day, end_day=end_day, metrics=metrics, time_zone_delta=time_zone_delta)

    night_data = extract_time_period(data, "02:00", "04:00")
    night_data["date"] = night_data["timestamp"].dt.date
    pivot_table = night_data.pivot_table(index="date", columns="deviceId", values="measurement", aggfunc="mean")
    return pivot_table

def retrieve_data_between_days(client: TellusClient, device_ids: list[str], start_day: str, end_day: str, metrics: list[str]=["sunrise.temperature"], time_zone_delta: int=-5):
    """Retrieve data for days specified.

    :param client: instantiated TellusClient object
    :param device_ids: devices to retrieve data from
    :param start_day: format YYYY-MM-DD
    :param end_day: format YYYY-MM-DD
    :param metrics: sensors to retrieve data from. sunrise is the default.
    :param time_zone_delta: hour offset for the target timezone

    :return complete_df: all data each day and device provided 
    """
    validate_date(start_day)
    validate_date(end_day)

    start_time = pd.to_datetime(start_day)
    start_time = start_time.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=time_zone_delta)))
    start_time = start_time.isoformat()

    end_time = pd.to_datetime(end_day) + pd.Timedelta(hours=23, minutes=59, seconds=59)
    end_time = end_time.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=time_zone_delta)))
    end_time = end_time.isoformat()

    responses = []
    for device_id in device_ids:
        data = client.retrieve_data(start_time, end_time, [device_id], metrics)
        if not data.empty:
            responses.append(data)

    complete_df = pd.concat(responses, ignore_index=True)
    return complete_df