import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt

t_regular = 1920.3047
t_hypertable = 3105.8773
t_conversion = 1868.793172

benchmark_times = {
    "regular → hypertable": t_regular,
    "hypertable": t_hypertable
}

conversion_times = {
    "regular → hypertable": t_conversion,
    "hypertable": 0
}

ghost = {
    "regular → hypertable": t_regular + t_conversion,
    "hypertable": t_hypertable

}

fig, ax = plt.subplots(figsize=(6, 6))

ax.bar(benchmark_times.keys(), benchmark_times.values(), label="Insertion time")
ax.bar(conversion_times.keys(), conversion_times.values(), bottom=list(benchmark_times.values()), label="Conversion time")

ax.set_ylabel("Wall clock time (minutes)")

# Plot ghost bars to get the stacked labels in the right spot.
bars = ax.bar(ghost.keys(), ghost.values(), alpha=0)
ax.bar_label(bars, fmt=lambda x: f"{x/60:.0f} mins")

ax.set_yticks(60 * np.array([0, 10, 20, 30, 40, 50, 60, 70]))
ax.yaxis.set_major_formatter(mpl.ticker.FuncFormatter(lambda x, _: f"{x/60:.0f}"))

ax.legend(frameon=False)

fig.savefig("conversion_time.png", dpi=200, transparent=False, bbox_inches="tight")
