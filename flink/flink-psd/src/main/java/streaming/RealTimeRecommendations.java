package streaming;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.locks.PredicateResults;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import redis.clients.jedis.Jedis;
import streaming.models.Review;
import streaming.models.ReviewDeserialization;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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

        DataStream<Review> ds = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source");
        SingleOutputStreamOperator<Tuple2<Long, Set<Long>>> userGroup = ds
                .map(new UserGroupRedisMapper())
                .map(new GroupUsersRedisMapper());

        final FileSink<String> sink = FileSink
                .forRowFormat(new Path("/home/psd/output"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(1)
                                .withInactivityInterval(1)
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();
        userGroup
                .map(
                    (MapFunction<Tuple2<Long, Set<Long>>, String>) val -> new String(val.f0.toString() + ":" + val.f1.toString())
                )
                .sinkTo(sink);
        env.execute();
    }
    public static class UserGroupRedisMapper extends RichMapFunction<Review, Tuple2<Long, Long>> {

        private transient Jedis jedis;

        @Override
        public Tuple2<Long, Long> map(Review review) {
            String userID = review.userId.toString();
            String userGroup = Objects.toString(jedis.get("userGroup:" + userID), "-1");
            return new Tuple2<>(Long.valueOf(userID), Long.valueOf(userGroup));
        }

        @Override
        public void open(Configuration parameters) {
            // open connection to Redis, for example
            jedis = new Jedis("localhost");
        }

        @Override
        public void close() {
            // close connection to Redis
            jedis.close();
        }
    }
    public static class GroupUsersRedisMapper extends RichMapFunction<Tuple2<Long, Long>, Tuple2<Long, Set<Long>>> {

        private transient Jedis jedis;

        @Override
        public Tuple2<Long, Set<Long>> map(Tuple2<Long, Long> userGroup) {
            Long userID = userGroup.f0;
            String groupID = userGroup.f1.toString();
            Set<String> members = jedis.smembers("groupUserSet:" + groupID);
            if (members == null)
                return new Tuple2<>(userID, Collections.emptySet());
            Set<Long> groupUsers = members
                    .stream()
                    .map(Long::valueOf)
                    .filter(val -> !val.equals(userID))
                    .collect(Collectors.toSet());
            return new Tuple2<>(userID, groupUsers);
        }

        @Override
        public void open(Configuration parameters) {
            // open connection to Redis, for example
            jedis = new Jedis("localhost");
        }

        @Override
        public void close() {
            // close connection to Redis
            jedis.close();
        }
    }
    public static class UserProductsMapper extends RichMapFunction<Tuple2<Long, Set<Long>>, Tuple2<Long, Set<Long>>> {

        private transient Jedis jedis;

        @Override
        public Tuple2<Long, Set<Long>> map(Tuple2<Long, Set<Long>> similiarUsers) {
            Long userID = similiarUsers.f0;
            Set<Long> similiarIDs = similiarUsers.f1;
            Set<Long> recommendedProducts = similiarIDs
                    .stream()
                    .flatMap(val -> ObjectUtils.firstNonNull(
                            jedis.smembers("userProducts:" + val.toString()),
                            Collections.<String>emptySet()
                            ).stream()
                    )
                    .map(Long::valueOf)
                    .collect(Collectors.toSet());
            
            return new Tuple2<>(userID, recommendedProducts);
        }

        @Override
        public void open(Configuration parameters) {
            // open connection to Redis, for example
            jedis = new Jedis("localhost");
        }

        @Override
        public void close() {
            // close connection to Redis
            jedis.close();
        }
    }
}
