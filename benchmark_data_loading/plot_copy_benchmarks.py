import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def parse_args():
    parser = argparse.ArgumentParser(
        description="Make bar plots of copy benchmarks."
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
        values=["rate_copy", "rate_full"],
        index=["method", "hypertable"],
        aggfunc=["mean", "median", "std", "min", "max"]
    )

    methods = pt.index.get_level_values("method").unique()

    def get(var, agg, hypertable):
        return pt[agg][var].xs(hypertable, level="hypertable").reindex(methods)

    def ranges(var, hypertable):
        return [
            get(var, "mean", hypertable) - get(var, "min", hypertable),
            get(var, "max", hypertable) - get(var, "mean", hypertable)            
        ]
        # return [
        #     get(var, "std", hypertable),
        #     get(var, "std", hypertable)
        # ]

    fig, ax = plt.subplots()

    x = np.arange(len(methods))
    width = 0.2  # the width of the bars

    ax.bar(
        x - 1.5*width,
        get("rate_copy", "mean", False),
        width,
        yerr=ranges("rate_copy", False),
        label="Copy rate (regular table)"
    )

    ax.bar(
        x - 0.5*width,
        get("rate_copy", "mean", True),
        width,
        yerr=ranges("rate_copy", True),
        label="Copy rate (hypertable)"
    )

    ax.bar(
        x + 0.5*width,
        get("rate_full", "mean", False),
        width,
        yerr=ranges("rate_full", False),
        label="Full rate (regular table)"
    )

    ax.bar(
        x + 1.5*width,
        get("rate_full", "mean", True),
        width,
        yerr=ranges("rate_full", True),
        label="Full rate (hypertable)"
    )

    ax.set_ylabel("Insert rate (rows per second)")
    ax.set_xticks(x)
    ax.set_xticklabels(methods)
    ax.legend(frameon=False)

    output_filename = Path(args.benchmarks_file).with_suffix(".png")
    plt.savefig(output_filename, dpi=100, transparent=False)

if __name__ == "__main__":
    main(parse_args())
