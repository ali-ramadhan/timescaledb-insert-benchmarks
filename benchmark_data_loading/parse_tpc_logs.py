import re
import csv
from pathlib import Path

# Convert e.g. "12m30s" to 750
def time_to_seconds(time_str):    
    match = re.match(r"(\d+)m(\d+(\.\d+)?)s", time_str)
    if match:
        minutes, seconds, _ = match.groups()
        return int(minutes) * 60 + float(seconds)
    
    match = re.match(r"(\d+)s", time_str)
    if match:
        return int(match.group(1))
    
    raise ValueError(f"Invalid time_str: {time_str}") 

# Function to parse file name for table type and number of workers
def parse_filename(filename):
    match = re.search(r"tpc_(regular|hyper)_(\d+)workers\.log", filename)
    if match:
        return match.group(1), match.group(2)
    raise ValueError(f"Invalid filename: {filename}")

def parse_log_file(log_filepath, output_csv_path=Path("benchmarks_parallel_tpc.csv")):
    table_type, num_workers = parse_filename(log_filepath)

    with open(output_csv_path, mode="a", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)

        if not output_csv_path.exists() or output_csv_path.stat().st_size == 0:
            csv_writer.writerow([
                "table_type",
                "num_workers",
                "time",
                "period_rate",
                "overall_rate",
                "total_rows"
            ])
    
        with open(log_filepath, "r") as file:
            for line in file:
                main_log_match = re.search(
                    r"at ([\dms]+), "
                    r"row rate ([\d\.]+)/sec \(period\), "
                    r"row rate ([\d\.]+)/sec \(overall\), "
                    r"([\d\.E\+]+) total rows",
                    line
                )

                last_line_match = re.search(
                    r"COPY (\d+), "
                    r"took ([\dms\.]+) "
                    r"with (\d+) worker\(s\) "
                    r"\(mean rate ([\d\.]+)/sec\)",
                    line
                )

                if main_log_match:
                    time_str, period_rate, overall_rate, total_rows = main_log_match.groups()
                    csv_writer.writerow([
                        table_type,
                        num_workers,
                        time_to_seconds(time_str),
                        period_rate,
                        overall_rate,
                        int(float(total_rows))
                    ])
                elif last_line_match:
                    total_rows, time_str, _, overall_rate = last_line_match.groups()
                    csv_writer.writerow([
                        table_type,
                        num_workers,
                        time_to_seconds(time_str),
                        overall_rate,
                        overall_rate,
                        int(float(total_rows))
                    ])

if __name__ == "__main__":
    for table_type in ["regular", "hyper"]:
        for n in [1, 2, 4, 8, 16, 32]:
            parse_log_file(f"tpc_{table_type}_{n}workers.log")

