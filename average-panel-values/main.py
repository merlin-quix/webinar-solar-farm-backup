from quixstreams import Application, State
from quixstreams.models.serializers.quix import JSONDeserializer
import os
import json
import logging
from datetime import datetime, timedelta
from quixstreams.dataframe.windows import Mean

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the Quix Application
app = Application(
    consumer_group="average-panel-values_v3",
    auto_create_topics=True,
    auto_offset_reset="latest"
)

# Define input and output topics
input_topic = app.topic(
    name=os.environ["input"],
    value_deserializer=JSONDeserializer()
)

output_topic = app.topic(
    name=os.environ["output"]
)

def process_message(value):
    """Extract and process the data from the message."""
    try:
        # The actual data is in the 'data' field
        data = value.get('data', {})
        if not data:
            return None
            
        # Parse timestamp if it's a string
        timestamp = value.get('timestamp')
        print("1")
        if isinstance(timestamp, str):
            try:
                timestamp = int(datetime.fromisoformat(timestamp).timestamp() * 1000000000)  # Convert to ns
            except (ValueError, TypeError):
                timestamp = int(datetime.now().timestamp() * 1000000000)
        
        return {
            'panel_id': data.get('panel_id'),
            'location_id': data.get('location_id'),
            'location_name': data.get('location_name'),
            'latitude': data.get('latitude'),
            'longitude': data.get('longitude'),
            'timezone': data.get('timezone'),
            'power_output': float(data.get('power_output', 0)),
            'temperature': float(data.get('temperature', 0)),
            'irradiance': float(data.get('irradiance', 0)),
            'voltage': float(data.get('voltage', 0)),
            'current': float(data.get('current', 0)),
            'timestamp': timestamp
        }
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None

from quixstreams.dataframe.windows import Aggregator

class PanelAggregator(Aggregator):
    def initialize(self):
        return {
            'power_output_sum': 0.0,
            'location_info': None,
            'location_panel_count': {},
            'location_panels': {}
        }

    def agg(self, old, new, ts):
        # Store location info from the first record
        if old['location_info'] is None:
            old['location_info'] = {
                'location_id': new.get('location_id'),
                'location_name': new.get('location_name'),
            }
        
        # Update metrics
        old['power_output_sum'] += float(new.get('power_output', 0))
        
        panel_id = new.get('panel_id')
        location_id = new.get('location_id')

        # Ensure location_id and panel_id are valid and hashable
        try:
            if (location_id is None or panel_id is None or 
                not isinstance(location_id, (str, int, float)) or 
                not str(location_id).strip()):
                logger.warning(f"Skipping record with invalid location_id or panel_id: {new}")
                return old
                
            # Convert location_id to string to ensure it's hashable
            location_id = str(location_id).strip()
            
            # Initialize location_panels as a dictionary if it doesn't exist
            if 'location_panels' not in old:
                old['location_panels'] = {}
            if location_id not in old['location_panels']:
                old['location_panels'][location_id] = []
                
            # Initialize location_panel_count as a dictionary if it doesn't exist
            if 'location_panel_count' not in old:
                old['location_panel_count'] = {}
            if location_id not in old['location_panel_count']:
                old['location_panel_count'][location_id] = 0
                
        except (TypeError, AttributeError) as e:
            logger.error(f"Error processing location_id {location_id}: {e}")
            return old

        try:
            # Add panel_id to the location's panel list if it's not already there
            if panel_id not in old['location_panels'][location_id]:
                old['location_panels'][location_id].append(panel_id)
                old['location_panel_count'][location_id] = len(old['location_panels'][location_id])
                
            # Debug output
            print(f"-- Updated state for location {location_id} --")
            print(f"Panel IDs: {old['location_panels'][location_id]}")
            print(f"Panel count: {old['location_panel_count'][location_id]}")
            
            return old
            
        except Exception as e:
            logger.error(f"Error updating panel data for location {location_id}: {e}")
            logger.error(f"Current state: {old}")
            return old

    def result(self, stored):
        if not stored['location_panel_count']:
            return None
            
        location = stored['location_info'] or {}
        location_id = location.get('location_id')
        
        # Calculate average power per panel for each location
        avg_power_per_panel = {}
        for loc_id, panel_count in stored['location_panel_count'].items():
            if panel_count > 0:
                avg_power_per_panel[loc_id] = stored['power_output_sum'] / panel_count
        
        # Return the average power for the current location
        return {
            'location_id': location_id,
            'location_name': location.get('location_name'),
            'avg_power_per_panel': avg_power_per_panel.get(location_id, 0),
            'panel_count': stored['location_panel_count'].get(location_id, 0),
            'timestamp': int(datetime.now().timestamp() * 1000)  # Add timestamp for reference
        }

# Create a streaming dataframe from the input topic
sdf = app.dataframe(input_topic)

## test window
# sdf["power"] = sdf["data"]["power_output"]
# sdf = sdf[["power"]]
# sdf = (
#     sdf.tumbling_window(timedelta(minutes=1))
#     .agg(p=Mean("power"))
#     .current()
# )
# sdf.print()


# Process each message to extract the data
sdf = sdf.apply(process_message)

# Define a 1-minute window and apply aggregation
window_size = timedelta(minutes=1)
# Apply the window and aggregation
sdf = (
    # sdf.group_by(lambda x: x.get('location_id') if x else None)
    sdf.tumbling_window(window_size)
    .agg(value=PanelAggregator())
    .current()
)

# Log the results
# sdf = sdf.update(
#     lambda value: logger.info(f"Processed window: {json.dumps(value, default=str)}") or value
# )

# sdf = sdf[["value"]]

def flatten_window_result(row):
    if not row or 'value' not in row:
        return None
    
    value = row['value']
    if not value:
        return None
        
    # Add window timestamps to the value
    value.update({
        'window_start': row.get('start'),
        'window_end': row.get('end')
    })
    return value

# Apply the flattening function
sdf = sdf.apply(flatten_window_result)

print(sdf)

# Send the result to the output topic
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    logger.info("Starting Average Panel Values service...")
    app.run(sdf)
