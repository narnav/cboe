import time
import csv
from datetime import datetime

def get_options_from_csv(file_path, start_time, start_date, call_put, strike):
    """
    Yield one option every 2 seconds that matches the provided criteria, sorted by time.

    :param file_path: Path to the CSV file containing the options data.
    :param start_time: Time to start filtering (HH:MM:SS).
    :param start_date: Date to filter (YYYY-MM-DD).
    :param call_put: 'C' for call, 'P' for put.
    :param strike: Desired strike price as a float.
    """
    start_time_obj = datetime.strptime(start_time, "%H:%M:%S").time()
    options = []

    # Step 1: Read all rows into a list
    with open(file_path, mode="r") as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        
        for row in reader:
            try:
                # Extract relevant fields
                option_date = row[1]
                option_time = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f")
                option_type = row[3]
                option_strike = float(row[2])
                
                # Add all rows with a valid time to the list
                options.append((option_date, option_time, option_type, option_strike, row))
                # print(len(options))
            except ValueError as e:
                print(f"Skipping row due to error: {e}")
                continue
            
    # Step 2: Sort the data by datetime
    options.sort(key=lambda x: x[1])  # Sort by option_time

    # Step 3: Filter and yield matching rows
    for option_date, option_time, option_type, option_strike, row in options:
        if (
            option_date == start_date
            # and option_time.time() >= start_time_obj
            and option_type == call_put
            and option_strike == strike
        ):
            yield row
            # time.sleep(1)  # Wait for 2 seconds before yielding the next result

# Example usage
if __name__ == "__main__":
    file_path = "data/xsp_2024-01.csv"
    start_time = "09:45:11"
    start_date = "2024-01-02"
    call_put = "C"
    strike = 475.000

    for option in get_options_from_csv(file_path, start_time, start_date, call_put, strike):
        print(option)
