package batch;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.graph.*;
import org.apache.flink.graph.library.LabelPropagation;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.*;

public class Recommendations {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Jedis jedis = new Jedis("localhost");
        FilterOperator < Tuple3 < Long, Long, Double >> reviews = getReviews(env, jedis);
        DataSet < Edge < Long, Double >> edgeList = reviews.map(new MapFunction < Tuple3 < Long, Long, Double > , Edge < Long, Double >> () {
            public Edge < Long, Double > map(Tuple3 < Long, Long, Double > e) {
                return new Edge < > (e.f0, e.f1, e.f2);
            }
        });
        Graph < Long, Long, Double > userTopProducts = getUserTopProducts(edgeList, env);
        DataSet < Edge < Long, NullValue >> similarUsers = getSimilarUsers(userTopProducts);
        Graph < Long, Long, NullValue > similarUsersGraph = extractGraph(similarUsers, env);
        DataSet < Tuple2 < Long, Long >> idsWithInitialLabels = initWithLabels(similarUsersGraph);
        DataSet < Vertex < Long, Long >> verticesWithCommunity = runLabelPropagation(similarUsersGraph, idsWithInitialLabels);

        Map < Long, Set < Long >> userProducts = new HashMap < > ();
        for (Tuple3 < Long, Long, Double > review: reviews.collect()) {
            Long userID = review.f0;
            Long productID = review.f1;
            userProducts.computeIfAbsent(userID, k -> new HashSet < > ()).add(productID);
        }
        Map < Long, Set < Long >> groupUsers = new HashMap < > ();
        for (Vertex < Long, Long > vertex: verticesWithCommunity.collect()) {
            Long userID = vertex.getId();
            Long groupID = vertex.getValue();
            groupUsers.computeIfAbsent(groupID, k -> new HashSet < > ()).add(userID);
        }


//        jedis
//                .keys("UserRecommendations:*")
//                .forEach(jedis::del);

        Transaction t = jedis.multi();
        Response<Set<String>> keys = t.keys("UserRecommendations:*");
        t.exec();

        t = jedis.multi();
        for (String key : keys.get()) {
            t.del(key);
        }
        t.exec();

        t = jedis.multi();
        for (Vertex < Long, Long > vertex: verticesWithCommunity.collect()) {
            Long userID = vertex.getId();
            Set < Long > selectedUserProducts = userProducts.get(userID);
            for (Long similiar: groupUsers.get(vertex.getValue()))
                for (Long productID: userProducts.get(similiar))
                    if (!selectedUserProducts.contains(productID))
                        t.sadd("UserRecommendations:" + userID, productID.toString());
        }
        t.exec();
        showDetectedCommunitiesSize(verticesWithCommunity);

        jedis.close();
    }

    private static void showDetectedCommunitiesSize(DataSet < Vertex < Long, Long >> verticesWithCommunity) throws Exception {
        verticesWithCommunity.groupBy(1).reduceGroup(new GroupReduceFunction < Vertex < Long, Long > , Tuple2 < Long, Long >> () {
            @Override
            public void reduce(Iterable < Vertex < Long, Long >> values, Collector < Tuple2 < Long, Long >> out) {
                long communityId = 0;
                long count = 0;

                for (Tuple2 < Long, Long > value: values) {
                    communityId = value.f1;
                    count++;
                }

                out.collect(new Tuple2 < > (communityId, count));
            }
        }).print();
    }

    private static DataSet < Vertex < Long, Long >> runLabelPropagation(Graph < Long, Long, NullValue > similarUsersGraph, DataSet < Tuple2 < Long, Long >> idsWithInitialLabels) throws Exception {
        return similarUsersGraph.joinWithVertices(idsWithInitialLabels, new VertexJoinFunction < Long, Long > () {
            public Long vertexJoin(Long vertexValue, Long inputValue) {
                return inputValue;
            }
        }).run(new LabelPropagation < > (5));
    }

    private static MapOperator < Tuple2 < Long, Long > , Tuple2 < Long, Long >> initWithLabels(Graph < Long, Long, NullValue > similarUsersGraph) {
        return DataSetUtils.zipWithUniqueId(similarUsersGraph.getVertexIds()).map(new MapFunction < Tuple2 < Long, Long > , Tuple2 < Long, Long >> () {
            public Tuple2 < Long, Long > map(Tuple2 < Long, Long > tuple2) {
                return new Tuple2 < > (tuple2.f1, tuple2.f0);
            }
        });
    }

    private static Graph < Long, Long, NullValue > extractGraph(DataSet < Edge < Long, NullValue >> similarUsers, ExecutionEnvironment env) {
        return Graph.fromDataSet(similarUsers, new MapFunction < Long, Long > () {
            public Long map(Long value) {
                return 1L;
            }
        }, env).getUndirected();
    }

    private static DistinctOperator < Edge < Long, NullValue >> getSimilarUsers(Graph < Long, Long, Double > userTopProducts) {
        return userTopProducts.getEdges().groupBy(1).reduceGroup(new CreateSimilarUserEdges()).distinct();
    }

    private static Graph < Long, Long, Double > getUserTopProducts(DataSet < Edge < Long, Double >> edgeList, ExecutionEnvironment env) {
        return Graph.fromDataSet(edgeList, new MapFunction < Long, Long > () {
            public Long map(Long value) {
                return value;
            }
        }, env);
    }

    private static FilterOperator < Tuple3 < Long, Long, Double >> getReviews(ExecutionEnvironment env, Jedis jedis) {
        ArrayList < Tuple3 < Long, Long, Double >> reviews = new ArrayList < Tuple3 < Long, Long, Double >> ();
        Transaction t = jedis.multi();
        Response<Set<String>> keys = t.keys("userRatings:*");
        t.exec();
        for (String users: keys.get()) {
            Long userID = Long.valueOf(users.split(":")[1]);
            for (String review: jedis.smembers(users)) {
                String[] parsed = review.split(":");
                Long productID = Long.valueOf(parsed[0]);
                Double rate = Double.valueOf(parsed[1]);
                reviews.add(
                        new Tuple3 < > (userID, productID, rate)
                );
            }
        }
        return env.fromCollection(reviews)
                .filter(value -> value.f0 < 1000)
                .filter(new FilterBadRatings());
    }

    private static final class CreateSimilarUserEdges implements GroupReduceFunction < Edge < Long, Double > , Edge < Long, NullValue >> {

        public void reduce(Iterable < Edge < Long, Double >> edges, Collector < Edge < Long, NullValue >> out) {
            List < Long > listeners = new ArrayList < > ();
            for (Edge < Long, Double > edge: edges) {
                listeners.add(edge.getSource());
            }
            for (int i = 0; i < listeners.size() - 1; i++) {
                for (int j = i + 1; j < listeners.size(); j++) {
                    out.collect(new Edge < > (listeners.get(i), listeners.get(j), NullValue.getInstance()));
                }
            }
        }
    }

    private static final class FilterBadRatings implements FilterFunction < Tuple3 < Long, Long, Double >> {
        public boolean filter(Tuple3 < Long, Long, Double > value) {
            return value.f2 > 3.0;
        }
    }
}