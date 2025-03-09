from confluent_kafka import Consumer, Producer, KafkaError
import json

def main(src_topic_name,dest_topic_name,servers,group_id):

    print("Step 2: Executing function defination")

    kafka_properties = {
        'bootstrap.servers': servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(kafka_properties)

    print("Step 3: Configuring source topic as: "+src_topic_name)

    consumer.subscribe([src_topic_name])

    print("Step 4: Configuring destination topic as: "+dest_topic_name)

    producer = Producer({'bootstrap.servers': servers})

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                try:
                    event = json.loads(msg.value().decode('utf-8'))
                    print("Received event:")
                    print(json.dumps(event, indent=4)) 

                    #ETL Logic 

                    #Produce to Destination Topic
                    producer.produce(dest_topic_name, json.dumps(event).encode('utf-8'))

                except Exception as e:
                    print("Error processing message:", e)

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close() 
        producer.flush()
        producer.close()  

if __name__ == '__main__':

    print("Step 1: Initialising Job")

    src_topic_name = 'src-debezium-cdc-workshop' ## Enter Your Source Topic Here 
    dest_topic_name = 'dest-flink-cdm-topic'     ## Enter Your Destination Topic Here 
    servers = 'kafka1:29092'                     ## Your localhost Ports
    group_id = 'cdc-flink-workshop'              ## Enter Counsmer Name You Want to Put.
    
    main(src_topic_name,dest_topic_name,servers,group_id)
