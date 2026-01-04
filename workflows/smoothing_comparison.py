import pandas as pd
import matplotlib.pyplot as plt
import scipy.signal
import seaborn as sns
import datetime
from math import ceil

DETRENDED_DATA_FILEPATH = "XXXXXX"
window_size = 50
OUTPUT_FILE_NAME = f"picaro_methane_smoothing_comparison_{int(window_size*6/60)}m"


def simple_moving_average(data: pd.Series, window_size: int) -> list[float]:
    """Calculate the SMA for a dataset.

    :param data: input data
    :param window_size: the number of values to be average
    """
    data_len = data.shape[0]

    def determine_indicies(i: int, window_size: int, data_length: int) -> tuple[int]:
        start_index = i - window_size//2
        end_index = i + window_size//2

        if start_index < 0: start_index = 0
        if end_index > data_length: end_index = data_length

        return start_index, end_index
    
    return [data.iloc[determine_indicies(i, window_size, data_len)[0]:determine_indicies(i, window_size, data_len)[1]].mean() for i in range(data_len)]

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
    picaro_data = pd.read_csv(DETRENDED_DATA_FILEPATH).drop("Unnamed: 0", axis=1)
    picaro_data["timestamp"] = pd.to_datetime(picaro_data["timestamp"])
    picaro_data["timestamp"] = picaro_data["timestamp"].apply(lambda entry: entry + datetime.timedelta(hours=-5))
    core_data = picaro_data[["timestamp", "CH4_dry", "latitude", "longitude"]].set_index("timestamp")

    ### STANDARDIZE TIMES ###
    frequency = pd.Timedelta("6 seconds")
    regular_intervals = core_data.resample(frequency).mean().interpolate()

    ### DENOISE ###
    sensor_freq = 1/6 #.167
    cut_off_freq = 1/120 # 1 oscillation per 1 minute
    regular_intervals["butterworth_smoothing"] = lowpass_butterworth(regular_intervals["CH4_dry"], sensor_freq, cut_off_freq)

    ### SMA ###
    regular_intervals["sma_smoothing"] = simple_moving_average(regular_intervals["CH4_dry"], window_size)

    ### EMA ###
    regular_intervals["ema_smoothing"] = regular_intervals["CH4_dry"].ewm(span=window_size, adjust=False).mean()

    
    sns.lineplot(data=regular_intervals, x="timestamp", y="CH4_dry", label="Raw Data")
    sns.lineplot(data=regular_intervals, x="timestamp", y="butterworth_smoothing", label="Butterworth")
    sns.lineplot(data=regular_intervals, x="timestamp", y="sma_smoothing", label="SMA")
    sns.lineplot(data=regular_intervals, x="timestamp", y="ema_smoothing", label="EMA")
    plt.xticks(rotation=30)
    plt.title("Picaro Methane Data (Smoothing Comparison)")
    plt.ylabel("Methane Delta (ppm)")
    plt.legend()
    plt.show()

    output_df = regular_intervals.reset_index()[["timestamp", "latitude", "longitude", "CH4_dry", "butterworth_smoothing", "sma_smoothing", "ema_smoothing"]]
    output_df.to_csv(f"{OUTPUT_FILE_NAME}.csv")
    print(f"{OUTPUT_FILE_NAME}.csv created")










    



