import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def round_to_significant_figures(x, sig=3):
    if x == 0:
        return 0
    else:
        return round(x, sig - int(np.floor(np.log10(np.abs(x)))) - 1)

df = pd.read_csv("benchmark_insert.csv")

pt = pd.pivot_table(df,
    values="rate",
    index=["method", "hypertable"],
    aggfunc=["mean", "std", "min", "max"]
) # .applymap(lambda x: round_to_significant_figures(x, 3))

fig, ax = plt.subplots()

methods = pt.index.get_level_values("method").unique()
x = np.arange(len(methods))
width = 0.35  # the width of the bars

error_margin_no_hypertable = [
    (pt["mean"]["rate"].xs(False, level="hypertable") - pt["min"]["rate"].xs(False, level="hypertable")),
    (pt["max"]["rate"].xs(False, level="hypertable") - pt["mean"]["rate"].xs(False, level="hypertable"))
]

error_margin_hypertable = [
    (pt["mean"]["rate"].xs(True, level="hypertable") - pt["min"]["rate"].xs(True, level="hypertable")),
    (pt["max"]["rate"].xs(True, level="hypertable") - pt["mean"]["rate"].xs(True, level="hypertable"))
]

ax.bar(
    x - width/2,
    pt["mean"]["rate"].xs(False, level="hypertable"),
    width,
    yerr=error_margin_no_hypertable,
    label="No Hypertable"
)

ax.bar(
    x + width/2,
    pt["mean"]["rate"].xs(True, level="hypertable"),
    width,
    yerr=error_margin_hypertable,
    label="Hypertable"
)

ax.set_ylabel("Inserts per second")
ax.set_xticks(x)
ax.set_xticklabels(methods)
ax.legend()

plt.show()