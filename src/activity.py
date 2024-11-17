import matplotlib.pyplot as plt
import csv
import matplotlib.dates as mdates
from datetime import datetime
import numpy as np

times = []
counts = []

with open("./../data/messages_by_time_of_day.csv", "r") as f:
  reader = csv.DictReader(f)
  for row in reader:
    times.append(datetime.strptime(row['time'], "%H:%M"))
    counts.append(int(row['count']))


window_size = 20
pad_left = (window_size - 1) // 2
pad_right = window_size // 2

padded_counts = np.pad(counts, (pad_left, pad_right), mode='edge')
smoothed_counts = np.convolve(padded_counts, np.ones(window_size)/window_size, mode='valid')

normalized_counts = smoothed_counts / np.max(smoothed_counts)

plt.figure(figsize=(10, 6))
plt.plot(times, normalized_counts)

plt.title("Server activity by time of day (Smoothed)")
plt.xlabel("Time of Day (UTC)")
plt.ylabel("Activity (Normalized)")

plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
# plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=1))  # Show one tick per hour
# plt.gca().xaxis.set_minor_locator(mdates.MinuteLocator(interval=30))  # Show minor ticks every 30 minutes

plt.grid(True, which='both', axis='x', color='gray', linestyle='-', linewidth=0.5)
plt.show()
