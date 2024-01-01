import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib as mpl
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

def p10(x):
    return np.percentile(x, 10)

def p90(x):
    return np.percentile(x, 90)

def main(args):
    df = pd.read_csv(args.benchmarks_file)

    pt = pd.pivot_table(df,
        values=["rate_copy", "rate_full"],
        index=["method", "table_type"],
        aggfunc=["median", p10, p90]
    )

    methods = pt.index.get_level_values("method").unique()

    def get(var, agg, table_type):
        return pt[agg][var].xs(table_type, level="table_type").reindex(methods)

    def ranges(var, table_type):
        return [
            get(var, "median", table_type) - get(var, "p10", table_type),
            get(var, "p90", table_type) - get(var, "median", table_type)            
        ]

    fig, ax = plt.subplots(figsize=(8, 6))

    x = np.arange(len(methods))
    width = 0.2  # the width of the bars

    ax.bar(
        x - 1.5*width,
        get("rate_copy", "median", "regular"),
        width,
        yerr=ranges("rate_copy", "regular"),
        capsize=5,
        label="Copy rate (regular table)"
    )

    ax.bar(
        x - 0.5*width,
        get("rate_copy", "median", "hyper"),
        width,
        yerr=ranges("rate_copy", "hyper"),
        capsize=5,
        label="Copy rate (hypertable)"
    )

    ax.bar(
        x + 0.5*width,
        get("rate_full", "median", "regular"),
        width,
        yerr=ranges("rate_full", "regular"),
        capsize=5,
        label="Full rate (regular table)"
    )

    ax.bar(
        x + 1.5*width,
        get("rate_full", "median", "hyper"),
        width,
        yerr=ranges("rate_full", "hyper"),
        capsize=5,
        label="Full rate (hypertable)"
    )

    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter("{x:,.0f}"))

    ax.set_ylabel("Insert rate (rows per second)")
    ax.set_xticks(x)
    ax.set_xticklabels(methods)
    ax.legend(frameon=False)

    output_filename = Path(args.benchmarks_file).with_suffix(".png")
    plt.savefig(output_filename, dpi=200, transparent=False)

if __name__ == "__main__":
    main(parse_args())
