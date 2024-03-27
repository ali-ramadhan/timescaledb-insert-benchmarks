import matplotlib as mpl
import matplotlib.pyplot as plt

def time_to_insert_rate(t_insert, t_overhead, num_rows):
    t_total = t_insert + t_overhead
    return num_rows / t_total

num_rows_1hour = 1_038_240
num_rows_256hours = 256 * num_rows_1hour
num_rows_31days = 31 * 24 * num_rows_1hour

t_csv_31days = 1068.49 # 16 workers (faster than 32)
t_csv_256hours = (256 / (31 * 24)) * t_csv_31days

insert_rate_pg_bulkload = time_to_insert_rate(1186.21, t_csv_256hours, num_rows_256hours)
insert_rate_pg_bulkload_parallel = time_to_insert_rate(849.89, t_csv_256hours, num_rows_256hours)
insert_rate_pg_bulkload_parallel_nofsync = time_to_insert_rate(2537.03, t_csv_31days, num_rows_31days)
insert_rate_tpc_1worker = time_to_insert_rate(2446.05018252, t_csv_256hours, num_rows_256hours)
insert_rate_tpc_32workers = time_to_insert_rate(1046.690627062, t_csv_256hours, num_rows_256hours)
insert_rate_tpc_16workers_nofsync = time_to_insert_rate(2045.17799286, t_csv_31days, num_rows_31days)
insert_rate_tpc_32workers_nofsync = time_to_insert_rate(2020.59743546, t_csv_31days, num_rows_31days)

# double check inexact values?
benchmarks = {
    "single-row\ninsert\npsycopg3": 2_600, 
    "multi-row\ninsert\npsycopg3": 27_000,
    "copy_csv\npostgres": 80_000,
    "copy\npsycopg3": 85_000,
    "tpc": insert_rate_tpc_1worker,
    "pgb": insert_rate_pg_bulkload,
    "tpc\n32W": insert_rate_tpc_32workers,
    "pgb\nparallel": insert_rate_pg_bulkload_parallel,
    "copy\npsycopg3\n32W": 241_641,
    "pgb\nparallel\nfsync off": insert_rate_pg_bulkload_parallel_nofsync,
    # "tpc\n16W\nfsync off": insert_rate_tpc_16workers_nofsync,
    "tpc\n32W\nfsync off": insert_rate_tpc_32workers_nofsync,
    # "copy\npsycopg3\n16W\nfsync off": 417_165,
    "copy\npsycopg3\n32W\nfsync off": 462_117,
}

def num_to_kM(val, pos):
    if val < 1e3:
        return f"{val:.0f}"
    if 1e3 <= val < 1e4:
        return f"{val/1e3:.1f}k"
    if 1e4 <= val < 1e6:
        return f"{val/1e3:.0f}k"
    elif 1e6 <= val < 1e9:
        return f"{val/1e6:.0f}M"

fig, ax = plt.subplots(figsize=(16, 8))
# fig, ax = plt.subplots(figsize=(8, 12))

bars = ax.bar(benchmarks.keys(), benchmarks.values())
# bars = ax.barh(list(benchmarks.keys()), list(benchmarks.values()))

ax.axvline(x=5.5, color="black", linestyle="dotted")
ax.axvline(x=8.5, color="black", linestyle="dotted")
# ax.axhline(y=5.5, color="black", linestyle="dotted")

ax.set_ylabel("Sustained insert rate including overhead (rows per second)")
# ax.set_xlabel("Sustained insert rate including overhead (rows per second)")

ax.bar_label(bars, fmt=mpl.ticker.FuncFormatter(num_to_kM))
ax.yaxis.set_major_formatter(mpl.ticker.FuncFormatter(num_to_kM))
# ax.xaxis.set_major_formatter(mpl.ticker.FuncFormatter(num_to_kM))

ax.set_xlim(-0.5, 11.5)
ax.set_ylim(0, 500e3)
# ax.set_xlim(0, 500e3)

# ax.invert_yaxis()

ax.text(2, 485e3, "single worker", fontsize=11, fontweight="bold")
ax.text(6.35, 485e3, "multiple workers", fontsize=11, fontweight="bold")
ax.text(8.90, 485e3, "multiple workers + fsync off", fontsize=11, fontweight="bold")

fig.savefig("benchmarks_summary.png", dpi=200, transparent=False, bbox_inches="tight")