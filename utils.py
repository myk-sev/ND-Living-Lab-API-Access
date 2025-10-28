import datetime, json, os, requests, sys, urllib3
from datetime import datetime as dt, timedelta
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
    mask = (data_times >= start_tobj) & (data_times <= end_tobj)

    # apply mask
    return data[mask]

def validate_date(date_str: str) -> None:
    """Ensures that the date provided is of "YYYY-MM-DD" format.

    :param date_str: the date to be checked
    :raises ValueError: occures when an incorrect format is found
    """
    try:
        datetime.datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Date must be in YYYY-MM-DD format, got: {date_str}")


def get_month_weeks(year: int, month: int) -> list[tuple[dt, dt]]:
    """Generate start and end datetime timestamps for each week in a specified month.

    :param year: e.g. 2024
    :param month: 1-12
    :return: start and end datetime object for each week in the month
    """
    # Get first and last day of the month
    first_day = dt(year, month, 1)
    next_month = dt(year + 1, 1, 1) if month == 12 else dt(year, month + 1, 1)
    last_day = next_month - timedelta(days=1)
    
    # Find the Sunday before or on the first day (weekday: 0=Mon, 6=Sun)
    week_start = first_day - timedelta(days=(first_day.weekday() + 1) % 7)
    
    weeks = []
    while week_start <= last_day:
        week_end = week_start + timedelta(days=6)
        
        # Count days in this week that fall within the target month
        days_in_month = sum(1 for i in range(7) 
                           if (week_start + timedelta(days=i)).month == month)
        
        # Include week if 4+ days are in the month
        if days_in_month >= 4:
            start_ts = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
            end_ts = week_end.replace(hour=23, minute=59, second=59, microsecond=999999)
            weeks.append((start_ts, end_ts))
        
        week_start += timedelta(days=7)
    
    return weeks


