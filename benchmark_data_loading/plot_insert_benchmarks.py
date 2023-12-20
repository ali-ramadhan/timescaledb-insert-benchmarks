import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("benchmark_insert.csv")

pt = pd.pivot_table(df,
    values="rate",
    index=["method", "hypertable"],
    aggfunc=["mean", "std", "min", "max"]
)

fig, ax = plt.subplots()

methods = pt.index.get_level_values("method").unique()
x = np.arange(len(methods))
width = 0.35  # the width of the bars

mean_rates_nht = pt["mean"]["rate"].xs(False, level="hypertable")
min_rates_nht = pt["min"]["rate"].xs(False, level="hypertable")
max_rates_nht = pt["min"]["rate"].xs(False, level="hypertable")

mean_rates_ht = pt["mean"]["rate"].xs(True, level="hypertable")
min_rates_ht = pt["min"]["rate"].xs(True, level="hypertable")
max_rates_ht = pt["min"]["rate"].xs(True, level="hypertable")

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
    label="No Hypertable"
)

ax.bar(
    x + width/2,
    mean_rates_ht,
    width,
    yerr=range_hypertable,
    label="Hypertable"
)

ax.set_ylabel("Inserts per second")
ax.set_xticks(x)
ax.set_xticklabels(methods)
ax.legend(frameon=False)

plt.savefig("insert_benchmarks.png", dpi=100, transparent=True)
