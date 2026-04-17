import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from pathlib import Path

# -----------------------------
# Exact data from your tables
# -----------------------------
workloads = ["Conversation", "Toolagent"]

data_48mb = {
    "original": [34.8, 78.3],           # original + legacy
    "original + SIEVE": [40.0, 78.3],   # SIEVE + legacy
    "radix + legacy": [38.1, 79.0],     # original + radix
    "radix + SIEVE": [49.4, 80.7],      # SIEVE + radix
}

data_36mb = {
    "original": [33.9, 78.3],           # original + legacy
    "original + SIEVE": [33.9, 78.3],   # SIEVE + legacy
    "radix + legacy": [35.0, 78.7],     # original + radix
    "radix + SIEVE": [43.8, 79.8],      # SIEVE + radix
}

# -----------------------------
# Style settings
# -----------------------------
blue = "#4C78A8"
green = "#59A14F"

series_order = [
    "original",
    "original + SIEVE",
    "radix + legacy",
    "radix + SIEVE",
]

series_style = {
    "original": {"color": blue, "hatch": None},
    "original + SIEVE": {"color": blue, "hatch": "///"},
    "radix + legacy": {"color": green, "hatch": None},
    "radix + SIEVE": {"color": green, "hatch": "///"},
}

x = np.arange(len(workloads))
width = 0.18

# -----------------------------
# Create figure
# -----------------------------
fig, axes = plt.subplots(1, 2, figsize=(10, 5), sharey=True)
fig.suptitle("Cache Hit Rate by Workload and Eviction Method", fontsize=14, y=0.98)

panel_info = [
    (axes[0], data_48mb, "Memory Budget: 48 MB"),
    (axes[1], data_36mb, "Memory Budget: 36 MB"),
]

for ax, panel_data, panel_title in panel_info:
    for i, label in enumerate(series_order):
        offset = (i - 1.5) * width
        style = series_style[label]

        ax.bar(
            x + offset,
            panel_data[label],
            width=width,
            color=style["color"],
            hatch=style["hatch"],
            edgecolor="white",
            linewidth=1.2,
        )

    ax.set_title(panel_title, fontsize=11)
    ax.set_xticks(x)
    ax.set_xticklabels(workloads, fontsize=11)
    ax.set_ylim(0, 100)
    ax.set_ylabel("Cache Hit Rate (%)", fontsize=11)
    ax.grid(axis="y", alpha=0.3)
    ax.set_axisbelow(True)

    # Cleaner look
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

# Keep y tick labels on right subplot too
axes[1].tick_params(labelleft=True)

# Custom legend so labels match exactly
legend_handles = [
    Patch(facecolor=blue, edgecolor="white", label="original"),
    Patch(facecolor=blue, edgecolor="white", hatch="///", label="original + SIEVE"),
    Patch(facecolor=green, edgecolor="white", label="radix + legacy"),
    Patch(facecolor=green, edgecolor="white", hatch="///", label="radix + SIEVE"),
]

fig.legend(
    handles=legend_handles,
    loc="upper center",
    ncol=4,
    frameon=False,
    bbox_to_anchor=(0.5, 0.92),
    fontsize=10,
)

plt.tight_layout(rect=[0, 0, 1, 0.88])
out_dir = Path(__file__).resolve().parent / "plots"
out_dir.mkdir(parents=True, exist_ok=True)
plt.savefig(out_dir / "cache_hit_rate_grouped.png", dpi=300, bbox_inches="tight")
