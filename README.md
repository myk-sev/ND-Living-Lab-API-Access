## ND-Living-Lab-API-Access

This repository retrieves time-series sensor data from three services — HOBOLink, Tellus, and LI-COR. Data can be retrieved from each service inidividually or merged into a single data frame for comparison.

## Quick start

### 1) Install dependencies
```bash
pip install -U python-dotenv pandas requests urllib3
```

### 2) Check environment variables
Important information such as secret keys, userids, and sensor serial numbers are stored in an external file, `.env`. This will have the following format:
```
### HOBO LINK SETTINGS ###
USER_ID=xxxx
LOGGER_ID=xxxxxxxx
STREAM_SITE_ID=xxxxxxxx

#Provided by Onset Tech support
CLIENT_ID=xxxxxx
CLIENT_SECRET=xxxxxxxxx

### TELLUS SETTINGS ###
TELLUS_KEY=xxxxxx
DEVICE_ID_FYE1=xxxxxx
DEVICE_ID_FYE2=xxxxxx
DEVICE_ID_CIL=xxxxxxx

### LICOR SETTING ###
LICOR_KEY=xxxxxxxx
DEVICE_ID_IRISH_ONE=xxxxxx
DEVICE_ID_IRISH_TWO=xxxxx
DEVICE_ID_IRISH_THREE=xxx-xxxxx
```

### 2) Fill user parameters
Settings such as the time periods for which to retrieve data and the data types to request are located at the top of `combo.py`.

```
START = "YYYY-mm-ddTHH:MM:SS+HH:MM"
END = "YYYY-mm-ddTHH:MM:SS+HH:MM"
TELLUS_METRICS = ["xxxxx", "xxxxx", "xxxxx", ...]
```
Dates are input utilizing the ISO 8601 standard. Any conversition to API specific formatting happens internally.


### 3) Test configuration and settings
API access for each platform is split into its own function. These require the start and end time of the request, along with platform specific information. To ensure these have been set correctly try running the script below:

```bash
df_hobolink = retrieve_data_hobolink(time_formatter(START), time_formatter(END))
df_hobolink.head()

df_tellus = retrieve_data_tellus(START, END, ["bme280.pressure", "sunrise.co2","pms5003t.d2_5"])
df_tellus.head()

df_licor = retrieve_data_licor(time_formatter(START), time_formatter(END), [IRISH_ONE, IRISH_TWO, IRISH_THREE])
df_licor.head()
```

This will show the first 5 enteries for each data set. Depending on the time stamps provided, HOBOLink may return an empty dataframe. This is ok and means the request was a success. If you would like to see different data try modifing the input.


## Notes
- Tellus requires specification of the metrics to be retrieved. To see all parameters available send a query to `/schema`. A helper function for this is included in [tellus-utils.py](https://github.com/myk-sev/ND-Living-Lab-API-Access/blob/main/combo.py).
- LICOR and HOBOLink utilize a different format for time entries. Be sure to call the helper function `time_formatter` on all inputs to their API requests.


## Additional Resources
- [Tellus-Starter-Guide](https://github.com/myk-sev/ND-Living-Lab-API-Access/blob/main/API-Starter-Guide.pdf)
- [HOBOlink® Web Services V3 Developer’s Guide](https://www.onsetcomp.com/resources/documentation/25113-hobolink-web-services-v3-developers-guide?srsltid=AfmBOoqP9aYBEM12HB8eTv7QaH9fuvtyQdb8YlDE41qoHiYIw684thIG)
- [Using the LICOR API](https://www.licor.com/support/Cloud/topics/using-the-api.html)
