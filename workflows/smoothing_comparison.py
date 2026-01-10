import pandas as pd
import matplotlib.pyplot as plt
import scipy.signal
import seaborn as sns
import datetime
from math import ceil

WINDOW_SIZE = 50
SAMPLING_RATE = 6 #seconds
STANDARDIZE_SAMPLING_INTERVAL = False
DETRENDED_DATA_FILEPATH = "xxxxxxxxx"
OUTPUT_FILE_NAME = f"picaro_methane_smoothing_comparison_{int(WINDOW_SIZE*SAMPLING_RATE/60)}m"


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

    if STANDARDIZE_SAMPLING_INTERVAL:
        ### STANDARDIZE TIMES ###
        frequency = pd.Timedelta(f"{SAMPLING_RATE} seconds")
        core_data = core_data.resample(frequency).mean().interpolate()

    ### DENOISE ###
    sensor_freq = 1/6 #.167
    cut_off_freq = 1/120 # 1 oscillation per 1 minute
    core_data["butterworth_smoothing"] = lowpass_butterworth(core_data["CH4_dry"], sensor_freq, cut_off_freq)

    ### SMA ###
    core_data["sma_smoothing"] = simple_moving_average(core_data["CH4_dry"], WINDOW_SIZE)

    ### EMA ###
    core_data["ema_smoothing"] = core_data["CH4_dry"].ewm(span=WINDOW_SIZE, adjust=False).mean()

    
    sns.lineplot(data=core_data, x="timestamp", y="CH4_dry", label="Raw Data")
    sns.lineplot(data=core_data, x="timestamp", y="butterworth_smoothing", label="Butterworth")
    sns.lineplot(data=core_data, x="timestamp", y="sma_smoothing", label="SMA")
    sns.lineplot(data=core_data, x="timestamp", y="ema_smoothing", label="EMA")
    plt.xticks(rotation=30)
    plt.title("Picaro Methane Data (Smoothing Comparison)")
    plt.ylabel("Methane Delta (ppm)")
    plt.legend()
    plt.show()

    output_df = core_data.reset_index()[["timestamp", "latitude", "longitude", "CH4_dry", "butterworth_smoothing", "sma_smoothing", "ema_smoothing"]]
    output_df.to_csv(f"{OUTPUT_FILE_NAME}.csv")
    print(f"{OUTPUT_FILE_NAME}.csv created")










    



