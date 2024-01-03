import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def main():
    df = pd.read_csv("benchmarks_parallel_copy.csv")

    def plot(method, table_type, label):
        num_workers = df.query(f"method == '{method}' and table_type == '{table_type}'")["workers"]
        insert_rate = df.query(f"method == '{method}' and table_type == '{table_type}'")["rate"]
        plt.plot(num_workers, insert_rate, marker='o', label=label)

    plot("psycopg3", "regular", label="psycopg3 (regular table)")
    plot("psycopg3", "hyper", label="psycopg3 (hypertable)")
    plot("copy_csv", "regular", label="COPY CSV (regular table)")
    plot("copy_csv", "hyper", label="COPY CSV (hypertable)")

    plt.xlabel("Workers")
    plt.ylabel("Insert rate (rows per second)")
    plt.legend()
    
    plt.savefig("benchmarks_parallel_copy.png", dpi=200, transparent=False)

if __name__ == "__main__":
    main()