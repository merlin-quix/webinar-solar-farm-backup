import os
from quixstreams import Application
from datetime import datetime

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

app = Application(consumer_group="enrichment-v1", 
                    auto_offset_reset="earliest", 
                    use_changelog_topics=False)

input_data_topic = app.topic(os.environ["data_topic"])
input_config_topic = app.topic(os.environ["config_topic"])
output_topic = app.topic(os.environ["output"])

data_sdf = app.dataframe(input_data_topic)
config_sdf = app.dataframe(input_config_topic)

last_config = {}

def save_config(data):
    global last_config
    last_config[data["location"]] = data
    # print("--config------")
    # print(last_config)
    # print("--------------")

def get_config_for_location(location):
    if location in last_config:
        return last_config[location]
    else:
        return {}

config_sdf.apply(save_config)

# debug
# data_sdf.apply(lambda row: print(row["location_id"]))

# Create nice JSON alert message.
data_sdf = data_sdf.apply(lambda row: {
    "timestamp": str(datetime.fromtimestamp(row["timestamp"]/1000/1000/1000)),
    "data": row,
    "configuration": get_config_for_location(row["location_id"])
})

# Print JSON messages in console.
# data_sdf.print()

# Send the message to the output topic
data_sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run()

# import os
# from quixstreams import Application
# from datetime import datetime

# # for local dev, load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# # Initialize the application with state management enabled
# app = Application(
#     consumer_group="enrichment-v3",
#     auto_offset_reset="earliest",
#     use_changelog_topics=True,  # Enable changelog topics for state management
#     state_dir="state",  # Local directory for state storage
#     consumer_extra_config={
#         "auto.offset.reset": "earliest",
#     },
# )

# input_data_topic = app.topic(os.environ["data_topic"])
# input_config_topic = app.topic(os.environ["config_topic"])
# output_topic = app.topic(os.environ["output"])

# data_sdf = app.dataframe(input_data_topic)
# config_sdf = app.dataframe(input_config_topic)


# def on_merge(left: dict, right: dict):
#     """
#     Merge left and right sides of the join into a new dictionary
#     """
#     timestamp = left.pop("timestamp")
#     date_str = str(datetime.fromtimestamp(timestamp/1000/1000/1000))
#     right['timestamp'] = str(datetime.fromtimestamp(right['timestamp']/1000/1000/1000))
#     return {"timestamp": date_str, "data": left, "config": right}


# # Group the dataframes with explicit store names
# data_sdf = data_sdf.group_by("location_id", name="data_group")
# config_sdf = config_sdf.group_by("location", name="config_group")

# data_sdf.print(metadata=True)
# config_sdf.print(metadata=True)

# # Join the latest effective config with the data
# # Using a named store for the join operation
# data_sdf = data_sdf.join_asof(
#     config_sdf,
#     on_merge=on_merge,
#     name="join_store"
# )

# # Send the message to the output topic
# # data_sdf.to_topic(output_topic)

# # data_sdf.print(metadata=True)
# data_sdf.print_table()


# if __name__ == "__main__":
#     app.run()