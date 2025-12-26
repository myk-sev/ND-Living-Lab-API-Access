import pandas as pd
from pathlib import Path
import sys
import csv
import os

DATA_DIR = Path("XXXXXXXX")

def convert_to_csv(input_path: str):
    """Convert space-separated file to CSV.
    
    :param input_path: path to the space seperated file (variable spacing allowable)
    """    
    output_filename = input_path.name.rsplit('.', 1)[0] + '.csv'
    output_path = DATA_DIR / output_filename
    with open(input_path, 'r') as infile, open(output_path, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        for line in infile:
            # Split on any whitespace and filter out empty strings
            row = [x for x in line.strip().split() if x]
            if row:  # Skip empty lines
                writer.writerow(row)
    
    return output_file

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python space_to_csv.py <input_file> [output_file.csv]")
        sys.exit(1)
        
    input_files = os.listdir(DATA_DIR)

    for input_file_name in input_files:
        input_path = DATA_DIR / input_file_name
        convert_to_csv(input_path)