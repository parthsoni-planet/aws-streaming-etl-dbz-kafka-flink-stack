#basic-watermarking-stream.py
#Third Approach
#Consume Kafka Events From Topic 
#using pyflink

import sys
import json
from datetime import datetime
from pyflink.common import Types, WatermarkStrategy, Time, Encoder
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream import TimeCharacteristic


class MyTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value, record_timestamp: int) -> int:
        event_json = json.loads(value)
        timestamp_str = event_json['transaction_creation_date']['utc']
        timestamp_obj = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
        timestamp_int = int(timestamp_obj.timestamp())
        return timestamp_int


def main(topic, group_id):
    try:
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)
        kafka_properties = {
            'bootstrap.servers': 'localhost:9092,localhost:9093,local:9094',
            'group.id': group_id
        }

        kafka_consumer = FlinkKafkaConsumer(
            topic,
            SimpleStringSchema(),  
            kafka_properties)  
        
        kafka_consumer.set_start_from_earliest()
        stream = env.add_source(kafka_consumer)

        # Assign timestamps to events and emit watermarks based on event timestamps
        stream_with_timestamps = stream.assign_timestamps_and_watermarks(
            WatermarkStrategy.for_monotonous_timestamps()
                .with_timestamp_assigner(MyTimestampAssigner())
        )

        def process_event(event):
            try:
                event_json = json.loads(event)
                print('Event Received:\n' + json.dumps(event_json, indent=4))
            except json.JSONDecodeError as e:
                print('Error decoding JSON:', e)

        stream.map(process_event)
        #stream.map(lambda x: print('Event Received:\n'+json.dumps(json.loads(x), indent=4)))
        
        env.execute("Consume Kafka Events")
    except Exception as e:
        print('An error occurred:', e)
    except KeyboardInterrupt:
        pass
    finally:
        print('Stopping Consumer')
        

if __name__ == '__main__':
    topic_name = 'dataeng-poc-flink-watermarking'
    group_id = 'flink-consumer-group'   
    main(topic_name, group_id)