import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import math
import os
import base64
from typing import List, Dict, Any, Optional
from datetime import datetime


def format_bytes(bytes_billed: Optional[int]) -> str:
    """
    Formats bytes into a human-readable string (Bytes, KB, MB, GB, TB, PB, EB, ZB, YB).

    Takes an optional integer representing the number of bytes and converts it
    into a more readable format with the appropriate unit (e.g., "1.23 MB").
    This function is useful for presenting byte counts in a user-friendly way
    in reports or emails. It handles cases where the input is None or zero.

    Args:
        bytes_billed: The number of bytes as an integer, or None if the value
                      is not available or applicable.

    Returns:
        A formatted string representing the byte count, including the unit.
        Returns "N/A" if the input is None.
    """
    if bytes_billed is None:
        return "N/A"
    if bytes_billed == 0:
        return "0 Bytes"
    size_name = ("Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    if bytes_billed < 1024:
         return f"{bytes_billed} Bytes"
    i = int(math.floor(math.log(bytes_billed, 1024)))
    p = math.pow(1024, i)
    s = round(bytes_billed / p, 2)
    return f"{s} {size_name[i]}"


def generate_spider_graph(scores: Optional[Dict[str, Optional[float]]], file_path: str, season_colors: Dict[str, str]) -> Optional[str]:
    """
    Generates a spider graph (radar chart) based on normalized scores.

    This function creates a radar chart visualizing performance across different
    score dimensions (e.g., security, cost, performance). It expects a dictionary
    of scores, where keys are the metric names and values are the corresponding
    scores normalized between 0 and 1. The graph is saved to a temporary file
    specified by `file_path`, and the path is returned upon successful generation.
    Seasonal colors are applied to the plot elements to match the email theme.

    Args:
        scores: A dictionary where keys are score metric names (str) and values
                are the corresponding scores (float or None), expected to be
                normalized between 0 and 1. If None or empty, graph generation
                is skipped.
        file_path: The full path (including filename and extension, e.g., /tmp/graph.png)
                   where the generated image should be saved temporarily. This path
                   should be accessible by the Cloud Function.
        season_colors: A dictionary containing color codes for graph elements,
                       based on the current season. Expected keys include
                       'graph_line' for plot lines and 'graph_fill' for filled areas.

    Returns:
        The `file_path` string if the graph was successfully generated and saved,
        None otherwise (e.g., if input data is insufficient, invalid, or saving fails).
    """
    if not scores:
        print("Warning: No score data provided for spider graph. Skipping generation.")
        return None

    valid_scores = {k: v for k, v in scores.items() if v is not None}

    if not valid_scores:
        print("Warning: No valid scores after filtering for spider graph. Skipping generation.")
        return None

    categories = list(valid_scores.keys())
    values = list(valid_scores.values())

    N = len(categories)
    if N == 0:
         print("Warning: No categories for spider graph. Skipping generation.")
         return None

    angles = [n / float(N) * 2 * np.pi for n in range(N)]
    angles += angles[:1]

    values += values[:1]

    fig, ax = plt.subplots(figsize=(6, 6), subplot_kw=dict(polar=True))

    ax.plot(angles, values, 'o-', linewidth=2, color=season_colors.get('graph_line', 'green'))
    ax.fill(angles, values, alpha=0.25, color=season_colors.get('graph_fill', 'lightgreen'))

    ax.set_thetagrids([a * 180/np.pi for a in angles[:-1]], categories)

    ax.set_title("Query Performance Snapshot", va='bottom')
    ax.grid(True)

    ax.set_ylim(0, 1)

    try:
        plt.savefig(file_path, format='png', bbox_inches='tight')
        plt.close(fig)
        return file_path
    except Exception as e:
        print(f"Error saving spider graph to {file_path}: {e}")
        plt.close(fig)
        return None


def generate_trendlines_graph(timestamps: List[datetime], grades: List[Optional[int]], dollars_per_job: List[Optional[float]], file_path: str, season_colors: Dict[str, str]) -> Optional[str]:
    """
    Generates a trendlines graph for grade and dollars billed per job over time.

    This function plots the historical trend of the user's overall grade and
    the calculated cost per job (dollars billed divided by jobs count) based on
    the provided data lists. It uses two y-axes if the scales of grade
    (0-100) and dollars per job differ significantly for better visualization.
    The graph is saved to a temporary file specified by `file_path`. Seasonal
    colors are applied to the plot elements.

    Args:
        timestamps: A list of datetime objects representing the time points for
                    each data point. Should be sorted in ascending order.
        grades: A list of optional integers representing the grade (0-100) at
                each corresponding timestamp. Can contain None values.
        dollars_per_job: A list of optional floats representing the calculated
                         dollars billed per job at each corresponding timestamp.
                         Can contain None values.
        file_path: The full path where the generated image should be saved temporarily.
                   This path should be accessible by the Cloud Function.
        season_colors: A dictionary containing color codes for graph elements,
                       based on the current season. Expected keys include 'graph_line'.

    Returns:
        The `file_path` string if the graph was successfully generated and saved,
        None otherwise (e.g., if input data is insufficient, invalid, or saving fails).
    """
    if not timestamps or len(timestamps) < 2:
        print("Warning: Not enough timestamps for trendlines (need at least 2). Skipping generation.")
        return None

    grades_nan = [g if g is not None else np.nan for g in grades]
    dollars_per_job_nan = [d if d is not None else np.nan for d in dollars_per_job]

    fig, ax1 = plt.subplots(figsize=(10, 5))

    color_grade = season_colors.get('graph_line', 'green')
    ax1.set_xlabel('Time')
    ax1.set_ylabel('Grade', color=color_grade)
    ax1.plot(timestamps, grades_nan, color=color_grade, marker='o', linestyle='-', label='Grade')
    ax1.tick_params(axis='y', labelcolor=color_grade)
    ax1.set_ylim(0, 100)

    ax2 = ax1.twinx()

    color_dollars_per_job = 'blue'
    ax2.set_ylabel('Dollars Billed / Job', color=color_dollars_per_job)
    ax2.plot(timestamps, dollars_per_job_nan, color=color_dollars_per_job, linestyle='--', marker='x', label='Dollars Billed / Job')
    ax2.tick_params(axis='y', labelcolor=color_dollars_per_job)

    fig.tight_layout()
    plt.title("Grade and Cost Per Job Trend Over Time")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines1 + lines2, labels1 + labels2, loc="upper left", bbox_to_anchor=(0.1, 0.9))

    fig.autofmt_xdate()

    try:
        plt.savefig(file_path, format='png', bbox_inches='tight')
        plt.close(fig) 
        return file_path
    except Exception as e:
        print(f"Error saving trendlines graph to {file_path}: {e}")
        plt.close(fig)
        return None


def get_image_base64(file_path: str) -> Optional[str]:
    """
    Reads an image file from the filesystem and encodes it into a Base64 string.

    This function is used to prepare image files (like generated graphs) for
    embedding directly into HTML email content using the data URI scheme.
    The Base64 string includes the necessary data URI prefix ('data:image/png;base64,').

    Args:
        file_path: The full path to the image file.

    Returns:
        A string containing the Base64 encoded image data prefixed with the
        data URI scheme, or None if the file does not exist or an error occurs
        during reading or encoding.
    """
    if not os.path.exists(file_path):
        return None
    try:
        with open(file_path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
        return f"data:image/png;base64,{encoded_string}"
    except Exception as e:
        print(f"Error encoding image {file_path} to Base64: {e}")
        return None
