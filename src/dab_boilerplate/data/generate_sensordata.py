from datetime import datetime, timedelta
import json
import random


data = []
start_date = datetime.now()
sensor_ids = [1001, 1002, 1003]  # Numeric sensor IDs starting from 1001

for hours in range(2):  # 2 hours of data
    for minutes in range(0, 60, 15):  # Data every 15 minutes
        for sensor_id in sensor_ids:
            timestamp = (
                start_date + timedelta(hours=hours, minutes=minutes)
            ).strftime("%Y-%m-%d %H:%M:%S")
            value = round(
                20 + 10 * random.random(), 2
            )  # Random temperature value between 20 and 30
            data.append(
                {
                    "timestamp": timestamp,
                    "sensor_id": sensor_id,
                    "value": value,
                }
            )

# Save data to a JSON file again with the updated sensor_id format
file_path = "./src/data/sample_data/sample_data_" + str(start_date) + ".json"
with open(file_path, "w") as file:
    json.dump(data, file, indent=4)
