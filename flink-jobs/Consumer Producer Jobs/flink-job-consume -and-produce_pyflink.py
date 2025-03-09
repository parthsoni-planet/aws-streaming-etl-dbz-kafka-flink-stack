#Second Approach
#Consume Kafka Events From Topic 
#using pyflink

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
import json

def main(src_topic_name,dest_topic_name,servers,group_id):
    print("Step 2: Executing function defination")
    try:
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)

        # env.add_jars("file:/opt/flink/lib/flink-connector-kafka-3.1.0-1.18.jar")
        # env.add_jars("file:/opt/flink/lib/kafka-clients-3.2.3.jar")

        # env.add_jars("file:/home/pyflink/flink-connector-kafka_2.11-1.14.6.jar")
        # env.add_jars("file:/home/pyflink/kafka-clients-2.4.1.jar")

        print("Step 3: Stream execution environment")

        kafka_properties = {
            'bootstrap.servers': servers,
            'group.id': group_id
        }

        print("Step 4: Configuring source topic as: "+src_topic_name)
        kafka_consumer = FlinkKafkaConsumer(
            src_topic_name,
            SimpleStringSchema(),  
            kafka_properties)  
        
        print("Step 5: Configuring destination topic as: "+dest_topic_name)
        kafka_producer = FlinkKafkaProducer(
            dest_topic_name,
            SimpleStringSchema(),  # Serialize back as simple string
            kafka_properties)
        
        kafka_consumer.set_start_from_earliest()

        stream = env.add_source(kafka_consumer)

        stream.add_sink(kafka_producer)
        
        def process_event(event):
            try:
                event_json = json.loads(event)
                print('Event Received:\n' + json.dumps(event_json, indent=4))
            except json.JSONDecodeError as e:
                print('Error decoding JSON:', e)

        stream.map(process_event)
        #stream.map(lambda x: print('Event Received:\n'+json.dumps(json.loads(x), indent=4)))
        
        print("Step 6: Submitting Job to Flink Jobmanager")

        env.execute("Consume Kafka Events from "+src_topic_name+" and Produce to "+dest_topic_name)
    except Exception as e:
        print('An error occurred:', e)
    except KeyboardInterrupt:
        pass
    finally:
        print('Stopping Consumer')
        

if __name__ == '__main__':
    print("Step 1: Initialising Job")
    src_topic_name = 'src-debezium-cdc-workshop' ## Enter Your Source Topic Here 
    dest_topic_name = 'dest-flink-cdm-topic'     ## Enter Your Destination Topic Here 
    servers = 'kafka1:29092,kafka2:29092,kafka3:29092' # Your localhost Ports
    group_id = 'cdc-flink-workshop'              ## Enter Counsmer Name You Want to Put.
    main(src_topic_name,dest_topic_name,servers,group_id)
