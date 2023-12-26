import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def parse_args():
    parser = argparse.ArgumentParser(
        description="Make bar plots of insert benchmarks."
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

    pt = pd.pivot_table(df,
        values="rate",
        index=["method", "hypertable"],
        aggfunc=["mean", "std", "min", "max"]
    )

    fig, ax = plt.subplots()

    methods = pt.index.get_level_values("method").unique()
    x = np.arange(len(methods))
    width = 0.35  # the width of the bars

    mean_rates_nht = pt["mean"]["rate"].xs(False, level="hypertable").reindex(methods)
    min_rates_nht = pt["min"]["rate"].xs(False, level="hypertable").reindex(methods)
    max_rates_nht = pt["max"]["rate"].xs(False, level="hypertable").reindex(methods)

    mean_rates_ht = pt["mean"]["rate"].xs(True, level="hypertable").reindex(methods)
    min_rates_ht = pt["min"]["rate"].xs(True, level="hypertable").reindex(methods)
    max_rates_ht = pt["max"]["rate"].xs(True, level="hypertable").reindex(methods)

    range_no_hypertable = [
        mean_rates_nht - min_rates_nht,
        max_rates_nht - mean_rates_nht
    ]

    range_hypertable = [
        mean_rates_ht - min_rates_ht,
        max_rates_ht - mean_rates_ht
    ]

    ax.bar(
        x - width/2,
        mean_rates_nht,
        width,
        yerr=range_no_hypertable,
        capsize=5,
        label="No Hypertable"
    )

    ax.bar(
        x + width/2,
        mean_rates_ht,
        width,
        yerr=range_hypertable,
        capsize=5,
        label="Hypertable"
    )

    ax.set_ylabel("Inserts per second")
    ax.set_xticks(x)
    ax.set_xticklabels(methods)
    ax.legend(frameon=False)

    output_filename = Path(args.benchmarks_file).with_suffix(".png")
    plt.savefig(output_filename, dpi=100, transparent=False)

if __name__ == "__main__":
    main(parse_args())
