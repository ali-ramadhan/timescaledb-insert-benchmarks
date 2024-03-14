import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt

def parse_args():
    parser = argparse.ArgumentParser(
        description="Plot results from copy-at-scale benchmarks."
    )

    parser.add_argument(
        "--benchmarks-file",
        type=str,
        help="Filepath to a benchmarks CSV file.",
        required=True
    )
    
    return parser.parse_args()

def main(args):
    df = pd.read_csv(args.benchmarks_file)

    def plot(ax, method, rate, table_type, label):
        dfq = df.query(f"method == '{method}' and table_type == '{table_type}'").sort_values(by="hour")
        rows_inserted = dfq["num_rows"].cumsum() / 1e6
        insert_rate = dfq[rate]
        insert_rate_smoothed = insert_rate.rolling(window=10, min_periods=1).mean()

        ax.scatter(rows_inserted, insert_rate, marker=".", alpha=0.2)
        ax.plot(rows_inserted, insert_rate_smoothed, label=label)

    fig, ax = plt.subplots(figsize=(8, 6))

    plot(ax, "psycopg3", "rate_full", "regular", label="psycopg3 (regular table)")
    plot(ax, "psycopg3", "rate_full", "hyper", label="psycopg3 (hypertable)")
    plot(ax, "copy_csv", "rate_full", "regular", label="COPY CSV (regular table)")
    plot(ax, "copy_csv", "rate_full", "hyper", label="COPY CSV (hypertable)")

    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter("{x:,.0f}"))

    ax.set_xlabel("Rows inserted (millions)")
    ax.set_ylabel("Insert rate (rows per second)")
    ax.legend(frameon=False, ncol=2, loc="upper center", bbox_to_anchor=(0.5, 1.15))
    
    output_filename = Path(args.benchmarks_file).with_suffix(".png")
    plt.savefig(output_filename, dpi=200, transparent=False)

if __name__ == "__main__":
    main(parse_args())
