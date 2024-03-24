import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt

def main():
    df = pd.read_csv("benchmarks_parallel_copy.csv")

    def plot(ax, method, table_type, label):
        num_workers = df.query(f"method == '{method}' and table_type == '{table_type}'")["workers"]
        insert_rate = df.query(f"method == '{method}' and table_type == '{table_type}'")["rate"]
        ax.plot(num_workers, insert_rate, marker='o', label=label)

    fig, ax = plt.subplots(figsize=(8, 6))

    plot(ax, "psycopg3", "regular", label="psycopg3 (regular table)")
    plot(ax, "psycopg3", "hyper", label="psycopg3 (hypertable)")
    plot(ax, "copy_csv", "regular", label="COPY CSV (regular table)")
    plot(ax, "copy_csv", "hyper", label="COPY CSV (hypertable)")

    def to_k(val, pos):
        return f"{val/1000:.0f}k"

    ax.yaxis.set_major_formatter(mpl.ticker.FuncFormatter(to_k))

    ax.set_xlabel("Workers")
    ax.set_ylabel("Insert rate (rows per second)")
    ax.legend(frameon=False, ncol=2, loc="upper center", bbox_to_anchor=(0.5, 1.15))
    
    fig.savefig("benchmarks_parallel_copy.png", dpi=200, transparent=False)

if __name__ == "__main__":
    main()