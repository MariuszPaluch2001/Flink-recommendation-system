package streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import streaming.models.KafkaOutput;
import streaming.models.KafkaOutputSerialization;
import streaming.models.Review;
import streaming.models.ReviewDeserialization;


import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class RealTimeRecommendations {
    public static long MAX_RECOMMENDATION_SIZE = 20;
    public static void main(String[] args) throws Exception {
        System.out.println("Real time recommendation");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        KafkaSource < Review > source = KafkaSource. < Review > builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("Reviews")
                .setGroupId("group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new ReviewDeserialization())
                .build();

        DataStream < Review > ds = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source");
        ds.addSink(new RedisSink < > (conf, new userRatingMapper()));
        SingleOutputStreamOperator < Tuple2 < String, Set < String >>> userGroup = ds
                .map(new RedisUserRecommendationMapping());

        SingleOutputStreamOperator < KafkaOutput > outputStream = userGroup
                .map(
                        (MapFunction < Tuple2 < String, Set < String >> , KafkaOutput > ) val -> new KafkaOutput(Long.valueOf(val.f0), val.f1)
                );

        KafkaRecordSerializationSchema < KafkaOutput > serializer = KafkaRecordSerializationSchema.builder()
                .setValueSerializationSchema(new KafkaOutputSerialization())
                .setTopic("Output")
                .build();

        KafkaSink < KafkaOutput > sink = KafkaSink. < KafkaOutput > builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(serializer)
                .build();

        outputStream.sinkTo(sink);

        env.execute();
    }
    public static class userRatingMapper implements RedisMapper < Review > {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SADD);
        }

        @Override
        public String getKeyFromData(Review data) {
            return "userRatings:" + data.userId;
        }

        @Override
        public String getValueFromData(Review data) {
            return data.productId + ":" + data.review;
        }
    }
    public static class RedisUserRecommendationMapping extends RichMapFunction < Review, Tuple2 < String, Set < String >>> {
        private transient Jedis jedis;

        @Override
        public Tuple2 < String,
                Set < String >> map(Review review) {
            String userID = review.userId.toString();
            Transaction t = jedis.multi();
            Response<Set<String>> tRecommend = t.smembers("UserRecommendations:" + userID);
            t.exec();
            Set < String > recommendations = tRecommend.get();

            t = jedis.multi();
            Response<Set<String>> tTopProducts = t.smembers("topProducts");;
            t.exec();
            recommendations = (recommendations != null && !recommendations.isEmpty()) ? recommendations : tTopProducts.get();
            recommendations = recommendations == null ? Collections.emptySet() : recommendations;
            recommendations = recommendations
                    .stream()
                    .filter(Objects::nonNull)
                    .limit(MAX_RECOMMENDATION_SIZE)
                    .collect(Collectors.toSet());
            return new Tuple2 < > (userID, recommendations);

        }

        @Override
        public void open(Configuration parameters) {
            jedis = new Jedis("localhost");
        }

        @Override
        public void close() {
            jedis.close();
        }
    }

}