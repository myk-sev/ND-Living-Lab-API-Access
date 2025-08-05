import datetime

def time_formatter_tellus(dt_str):
    guide = '%Y-%m-%dT%H:%M:%S%+%H:%M'
    dt_obj = datetime.datetime.strptime(dt_str, guide)
    tellus_time = datetime.datetime.strftime()