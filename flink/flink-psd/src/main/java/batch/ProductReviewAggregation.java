package batch;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Vertex;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;

public class ProductReviewAggregation {
    private static final int topProductsNumber = 20;
    private static final int minReviewsNumber = 10;

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();

        Jedis jedis = new Jedis("localhost");

        DataSet<Tuple2<Long, Double>> productData = getProductReviews(env, jedis);
        DataSet<Tuple2<Long, Double>> aggregateData = productData.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
            @Override
            public void reduce(Iterable<Tuple2<Long, Double>> values, Collector<Tuple2<Long, Double>> out) {
                Long productId = 0L;
                Double sum = 0.0;
                int count = 0;
                for (Tuple2<Long, Double> review : values) {
                    productId = review.f0;
                    sum += review.f1;
                    count++;
                }
                if(count > minReviewsNumber)
                    out.collect(new Tuple2<>(productId, sum / count));
            }
        }).sortPartition(1, Order.DESCENDING).first(topProductsNumber);

        jedis.del("topProducts");

        DataStreamSource<Tuple2<Long, Double>> stream = env2.fromCollection(aggregateData.collect());
        stream.addSink(new RedisSink<>(conf, new RedisTopProductAdder()));
        env2.execute();

        aggregateData.print();
    }

    private static DataSource<Tuple2<Long, Double>> getProductReviews(ExecutionEnvironment env, Jedis jedis) {
        ArrayList<Tuple2<Long, Double>> reviews =  new ArrayList<Tuple2<Long, Double>>();
        for (String users : jedis.keys("userRatings:*")) {
            for (String review : jedis.smembers(users)) {
                String [] parsed = review.split(":");
                Long productID = Long.valueOf(parsed[0]);
                Double rate = Double.valueOf(parsed[1]);
                reviews.add(
                        new Tuple2<>(productID, rate)
                );
            }
        }
        return env.fromCollection(reviews);

        //        return env.readCsvFile(recommendationsInputPath)
//                .lineDelimiter("\n")
//                .fieldDelimiter(",")
//                .includeFields(false, true, true, false)
//                .types(Long.class, Double.class);
    }

    public static class RedisTopProductAdder implements RedisMapper<Tuple2<Long, Double>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SADD);
        }

        @Override
        public String getKeyFromData(Tuple2<Long, Double> data) {
            return "topProducts";
        }

        @Override
        public String getValueFromData(Tuple2<Long, Double> data) {
            return data.f0.toString();
        }
    }
}