import logging
import sys
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer
from pyflink.datastream.formats.json import (
    JsonRowSerializationSchema,
    JsonRowDeserializationSchema,
)
from datetime import datetime
import random
import time
import csv

DATA_STREAM_FILE = "../data/stream.edges"
TOPIC_NAME = "Reviews"
TIME_SLEEP_IN_SECONDS = 1


def generate_temp():
    with open(DATA_STREAM_FILE, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            yield (int(row[0]), int(row[1]), float(row[2]), int(row[3]))


def write_to_kafka(env):
    type_info = Types.ROW_NAMED(["userId", "productId", "review", "timestamp"], [Types.LONG(), Types.LONG(), Types.DOUBLE(), Types.LONG()])
    serialization_schema = (
        JsonRowSerializationSchema.Builder().with_type_info(type_info).build()
    )
    kafka_producer = FlinkKafkaProducer(
        topic=TOPIC_NAME,
        serialization_schema=serialization_schema,
        producer_config={"bootstrap.servers": "localhost:9092", "group.id": "group"},
    )
    for data in generate_temp():
        time.sleep(TIME_SLEEP_IN_SECONDS)
        ds = env.from_collection([data], type_info=type_info)
        ds.add_sink(kafka_producer)
        env.execute()


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(
        "file:///home/psd/Downloads/flink-connector-kafka-3.1.0/flink-sql-connector-kafka/target/flink-sql-connector-kafka-3.1.0.jar"
    )
    print("Start writing reviews to kafka.")
    write_to_kafka(env)

