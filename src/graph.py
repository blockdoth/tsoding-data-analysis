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

plt.figure(figsize=(10, 6))
plt.plot(times, smoothed_counts)

plt.title("Messages Sent by Time of Day")
plt.xlabel("Time of Day")
plt.ylabel("Message count")

plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
# plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=1))  # Show one tick per hour
# plt.gca().xaxis.set_minor_locator(mdates.MinuteLocator(interval=30))  # Show minor ticks every 30 minutes


plt.show()
