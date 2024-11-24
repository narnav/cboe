import time
import csv
from datetime import datetime

def get_options_from_csv(file_path, start_time, start_date, call_put, strike):
    """
    Yield one option every 2 seconds that matches the provided criteria from a CSV file.

    :param file_path: Path to the CSV file containing the options data.
    :param start_time: Time to start filtering (HH:MM:SS).
    :param start_date: Date to filter (YYYY-MM-DD).
    :param call_put: 'C' for call, 'P' for put.
    :param strike: Desired strike price as a float.
    """
    start_time_obj = datetime.strptime(start_time, "%H:%M:%S").time()

    with open(file_path, mode="r") as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            # Extract relevant fields
            option_date = row[4]
            option_time = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S.%f").time()
            option_type = row[6]
            option_strike = float(row[5])

            # Filter options based on criteria
            if (
                option_date == start_date
                # and option_time >= start_time_obj
                # and option_type == call_put
                and  option_strike == strike
            ):
                yield row
                time.sleep(1)  # Wait for 2 seconds before yielding the next result

# Example usage
if __name__ == "__main__":
    file_path = "data/xsp_2024-02.csv"
    start_time = "11:40:00"
    start_date = "2024-02-01"
    call_put = "C"
    strike = 477.000

    for option in get_options_from_csv(file_path, start_time, start_date, call_put, strike):
        print(option)
