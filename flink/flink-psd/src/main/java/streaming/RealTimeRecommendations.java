package streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import streaming.models.Review;
import streaming.models.ReviewDeserialization;
public class RealTimeRecommendations {
    public static void main(String[] args) throws Exception {
        System.out.println("Real time recommendation");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<Review> source = KafkaSource.<Review>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("Reviews")
                .setGroupId("group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new ReviewDeserialization())
                .build();

        DataStream<Review> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<String> output = ds.map(
                new MapFunction<Review, String>() {
                    @Override
                    public String map(Review value) throws Exception {
                        return value.userId.toString();
                    }
                }
        );
//        ds.print();

//        KafkaSource<String> source = KafkaSource.<String>builder()
//                .setBootstrapServers("localhost:9092")
//                .setTopics("Test")
//                .setGroupId("group")
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();

        KafkaRecordSerializationSchema<String> serializer = KafkaRecordSerializationSchema.builder()
                .setValueSerializationSchema(new SimpleStringSchema())
                .setTopic("Output")
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(serializer)
                .build();

        output.sinkTo(sink);
        env.execute();
    }
}
