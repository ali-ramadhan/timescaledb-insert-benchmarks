import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def main(args):
    df = pd.read_csv("benchmarks_copy_at_scale.csv")

    def plot(method, rate, hypertable, label):
    	rows_inserted = df.query(f"method == '{method}' and hypertable == {hypertable}")["num_rows"].cumsum()
    	insert_rate = df.query(f"method == '{method}' and hypertable == {hypertable}")[rate]
    	insert_rate_smoothed = insert_rate.rolling(10).mean()

    	plt.scatter(rows_inserted / 1e6, insert_rate, marker=".", alpha=0.5, label=label)
    	plt.plot(rows_inserted / 1e6, insert_rate_smoothed, label=label)

    plot("psycopg3", "rate_copy", False, label="psycopg3 + regular table")
    plot("psycopg3", "rate_copy", True, label="psycopg3 + hypertable")
    plot("copy_csv", "rate_copy", False, label="COPY + regular table")

    plt.xlabel("Rows inserted (millions)")
    plt.ylabel("Insert rate (rows per second)")
    plt.legend()
    
    plt.savefig("benchmarks_copy_at_scale.png", dpi=200, transparent=False)

if __name__ == "__main__":
    main(None)