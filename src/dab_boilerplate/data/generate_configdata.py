import json

# Generate sample configuration data for the sensors
sensor_configs = [
    {
        "sensor_name": "temp_sensor_A",
        "sensor_id": 1001,
        "sensor_type": "Temperature",
        "unit": "Celsius",
    },
    {
        "sensor_name": "temp_sensor_B",
        "sensor_id": 1002,
        "sensor_type": "Temperature",
        "unit": "Celsius",
    },
    {
        "sensor_name": "temp_sensor_C",
        "sensor_id": 1003,
        "sensor_type": "Temperature",
        "unit": "Celsius",
    },
]

# Save config data to a JSON file
config_file_path = "./src/data/sample_data/config_data_.json"
with open(config_file_path, "w") as file:
    json.dump(sensor_configs, file, indent=4)

config_file_path
