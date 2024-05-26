package streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

        DataStream<Review> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        stream.print();

        env.execute();
    }
}
