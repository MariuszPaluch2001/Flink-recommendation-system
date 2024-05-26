package batch;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.List;

public class InitialInsert {
    private static String recommendationsInputPath = null;

    public static void main(String[] args) throws Exception {
        if (!parseParameters(args)) {
            System.out.println("Path to data file doesn't specified in args.");
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();

        List<Tuple3<Long, Long, Double>> initialData = env.readCsvFile(recommendationsInputPath)
                .lineDelimiter("\n")
                .fieldDelimiter(",")
                .types(Long.class, Long.class, Double.class)
                .filter(data -> data.f0 < 1000)
                .collect();
        DataStreamSource<Tuple3<Long, Long, Double>> initialDataStream = env2.fromCollection(initialData);
        initialDataStream.addSink(new RedisSink<>(conf, new RedisUserProductsMapper()));
        env2.execute();
    }

    private static boolean parseParameters(String[] args) {
        recommendationsInputPath = args[0];
        return true;
    }

    public static class RedisUserProductsMapper implements RedisMapper<Tuple3<Long, Long, Double>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SADD);
        }

        @Override
        public String getKeyFromData(Tuple3<Long, Long, Double>data) {
            return "userProductsSet:" + data.f0.toString();
        }

        @Override
        public String getValueFromData(Tuple3<Long, Long, Double> data) {
            return data.f1.toString() + ":" + data.f2.toString();
        }
    }
}
