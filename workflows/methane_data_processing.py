import pandas as pd
import matplotlib.pyplot as plt
import scipy.signal
import seaborn as sns
import datetime
from math import ceil

PICARO_DATA_INPUT_PATH = "C:\\Users\\sevmy\\OneDrive\\Documents\\ND\\Golf Cart\\picaro_data\\picaro_sept_15.csv"
OUTPUT_PATH = "picaro_methane.csv"

def lowpass_butterworth(data: pd.Series, sampling_freq: int, cutoff_freq: int) -> pd.Series:
    """Apply a buttworth filter to extract out signal below a specified cutt off frequency.

    :param data: sensor data. regular time intervals required
    :param sampling_freq: frequency of data recording (Hz)
    :param cutoff_freq: frequency below which data is removed (Hz)

    :return: background drift
    """
    nyquist_freq = sampling_freq/2
    normalized_cutt_off = cutoff_freq/nyquist_freq

    b, a = scipy.signal.butter(
        N=3, 
        btype="lowpass", 
        Wn=normalized_cutt_off
    )

    extracted_signal = scipy.signal.filtfilt(b, a, data) #filter applied forward & backward to remove time shift
    return extracted_signal
        
if __name__ == "__main__":
    ### IMPORT DATA ###
    picaro_data = pd.read_csv(PICARO_DATA_FILEPATH).drop("Unnamed: 0", axis=1)
    picaro_data["timestamp"] = pd.to_datetime(picaro_data["timestamp"])
    picaro_data["timestamp"] = picaro_data["timestamp"].apply(lambda entry: entry + datetime.timedelta(hours=-5))
    core_data = picaro_data[["timestamp", "CH4_dry", "CO2_dry", "GPS_ABS_LAT", "GPS_ABS_LONG"]].set_index("timestamp")

    ### STANDARDIZE TIMES ###
    frequency = pd.Timedelta("6 seconds")
    regular_intervals = core_data.resample(frequency).mean().interpolate()

    ### DENOISE ###
    sensor_freq = 1/6 #.167
    cut_off_freq = 1/120 # 1 oscillation per 1 minute
    regular_intervals["denoised"] = lowpass_butterworth(regular_intervals["CH4_dry"], sensor_freq, cut_off_freq)

    ### BACKGROUND DRIFT EXTRACTION ###
    sensor_freq = 1/6 #.167
    cut_off_freq = 1/7200 # 1 oscillation per 3 hours
    regular_intervals["CH4_background"] = lowpass_butterworth(regular_intervals["denoised"], sensor_freq, cut_off_freq)
    regular_intervals["CH4_delta"] = regular_intervals["denoised"] - regular_intervals["CH4_background"]

    sns.lineplot(data=regular_intervals, x="timestamp", y="CH4_delta")
    plt.xticks(rotation=30)
    plt.title("Picaro Methane Data (Background Removed, Lowpass Smoothing)")
    plt.ylabel("Methane Delta (ppm)")

    output_df = regular_intervals.reset_index()[["timestamp", "GPS_ABS_LAT", "GPS_ABS_LONG", "CH4_delta"]]
    output_df = output_df.rename(columns={
        "GPS_ABS_LAT":"Latitude",
        "GPS_ABS_LONG":"Longitude",
        "CH4_delta":"CH4",
        })
    output_df.to_csv("picaro_methane.csv")










    



