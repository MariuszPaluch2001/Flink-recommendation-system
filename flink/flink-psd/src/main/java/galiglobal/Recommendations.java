package galiglobal;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.graph.*;
import org.apache.flink.graph.library.LabelPropagation;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class Recommendations {
    private static String recommendationsInputPath = null;

    public static void main(String[] args) throws Exception{
        if (!parseParameters(args)) {
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Edge<Long, Double>> edgeList = env.readCsvFile(recommendationsInputPath)
                .lineDelimiter("\n")
                .fieldDelimiter(",")
                .types(Long.class, Long.class, Double.class)
                .filter(new FilterFunction<Tuple3<Long, Long, Double>>() {
                    @Override
                    public boolean filter(Tuple3<Long, Long, Double> value) throws Exception {
                        return value.f0 < 1000;
                    }
                })
                .filter(new FilterBadRatings())
                .map(
                new MapFunction<Tuple3<Long, Long, Double>, Edge<Long, Double>>() {
                    public Edge<Long, Double> map(Tuple3<Long, Long, Double> e) {
                        return new Edge<>(e.f0, e.f1, e.f2);
                    }
                }
        );

        edgeList.print();

        Graph<Long, Long, Double> userTopProducts = Graph.fromDataSet(edgeList,
                new MapFunction<Long, Long>() {
                    public Long map(Long value) {
                        return value;
                    }
                }, env);

        DataSet<Edge<Long, NullValue>> similarUsers =
                userTopProducts
                        .getEdges()
                        .groupBy(1)
                        .reduceGroup(new Recommendations.CreateSimilarUserEdges())
                        .distinct();

        Graph<Long, Long, NullValue> similarUsersGraph =
                Graph.fromDataSet(
                                similarUsers,
                                new MapFunction<Long, Long>() {
                                    public Long map(Long value) {
                                        return 1L;
                                    }
                                },
                                env)
                        .getUndirected();

        DataSet<Tuple2<Long, Long>> idsWithInitialLabels =
                DataSetUtils.zipWithUniqueId(similarUsersGraph.getVertexIds())
                        .map(
                                new MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                                    public Tuple2<Long, Long> map(Tuple2<Long, Long> tuple2) {
                                        return new Tuple2<>(tuple2.f1, tuple2.f0);
                                    }
                                });

        DataSet<Vertex<Long, Long>> verticesWithCommunity =
                similarUsersGraph
                        .joinWithVertices(
                                idsWithInitialLabels,
                                new VertexJoinFunction<Long, Long>() {
                                    public Long vertexJoin(Long vertexValue, Long inputValue) {
                                        return inputValue;
                                    }
                                })
                        .run(new LabelPropagation<>(10));

        verticesWithCommunity.print();
        System.out.println(verticesWithCommunity.count());

    }

    private static DataSet<Tuple4<Long, Long, Double, Long>> getRecommendations(ExecutionEnvironment env) {
        return env.readCsvFile(recommendationsInputPath)
                .lineDelimiter("\n")
                .fieldDelimiter(",")
                .types(Long.class, Long.class, Double.class, Long.class);
        }

    private static boolean parseParameters(String[] args) {
        recommendationsInputPath = args[0];
        return true;
    }

    private static final class CreateSimilarUserEdges
            implements GroupReduceFunction<Edge<Long, Double>, Edge<Long, NullValue>> {

        public void reduce(
                Iterable<Edge<Long, Double>> edges, Collector<Edge<Long, NullValue>> out) {
            List<Long> listeners = new ArrayList<>();
            for (Edge<Long, Double> edge : edges) {
                listeners.add(edge.getSource());
            }
            for (int i = 0; i < listeners.size() - 1; i++) {
                for (int j = i + 1; j < listeners.size(); j++) {
                    out.collect(
                            new Edge<>(
                                    listeners.get(i), listeners.get(j), NullValue.getInstance()));
                }
            }
        }
    }

    private static final class RemoveColumn extends RichMapFunction<Tuple4<Long, Long, Double, Long>, Tuple3<Long, Long, Double>> {
        public Tuple3<Long, Long, Double> map(Tuple4<Long, Long, Double, Long> value) {
            return new Tuple3<>(value.f0, value.f1, value.f2);
        }
    }

    private static final class FilterBadRatings implements FilterFunction<Tuple3<Long, Long, Double>> {
        public boolean filter(Tuple3<Long, Long, Double> value) throws Exception {
            return value.f2 > 3.0;
        }
    }
}
