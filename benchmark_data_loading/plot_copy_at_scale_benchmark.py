import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def main(args):
    df = pd.read_csv("benchmarks_copy_at_scale.csv")
    
    rows_inserted = df.query("hypertable == False")["num_rows"].cumsum()
    insert_rate_rt = df.query("hypertable == False")["rate_copy"]
    insert_rate_ht = df.query("hypertable == True")["rate_copy"]

    plt.plot(rows_inserted, insert_rate_rt, label="Regular table")
    plt.plot(rows_inserted, insert_rate_ht, label="Hypertable")
    
    plt.savefig("benchmarks_copy_at_scale.png", dpi=100, transparent=False)