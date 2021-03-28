package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Exercise_3 {

    private static class VProg extends AbstractFunction3<Long,Tuple2<Integer,List<Long>>,Tuple2<Integer,List<Long>>,Tuple2<Integer, List<Long>>> implements Serializable {
        @Override
        public Tuple2<Integer,List<Long>> apply(Long vertexID,Tuple2<Integer,List<Long>> vertexValue, Tuple2<Integer,List<Long>> message) {
            if (message._1 == Integer.MAX_VALUE) {             // superstep 0
                // Assigning the ID of the Vertex we are going to start from:
                vertexValue._2.add(1l);
                return vertexValue;
            }
            else {                                             // superstep > 0
                // When receiving a message (made of a Tuple2<Integer, List<Long>>), it means that I have
                // been reached because I am in the shortest path, so I, as a vertex, append my vertex ID
                // to the existing list I am receiving from my neighbour vertex, and I update both my
                // shortest-path value as well as the existing list with the vertices up to me (vertex):
                message._2.add(vertexID);
                return message;
            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Tuple2<Integer,List<Long>>,Integer>, Iterator<Tuple2<Object,Tuple2<Integer,List<Long>>>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Tuple2<Integer,List<Long>>>> apply(EdgeTriplet<Tuple2<Integer,List<Long>>, Integer> triplet) {
            Tuple2<Object,Tuple2<Integer,List<Long>>> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,Tuple2<Integer,List<Long>>> dstVertex = triplet.toTuple()._2();
            Integer edge = triplet.toTuple()._3();

            if (sourceVertex._2._1 >= dstVertex._2._1) {   // source vertex shortest-path value is greater or equal to dst vertex?
                // Do nothing. We don't want to update anything:
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Tuple2<Integer,List<Long>>>>().iterator()).asScala();
            } else {
                // Propagate source vertex value to Adjacent Nodes, as well as the existing list of the source vertex with the shortest path so far:
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Tuple2<Integer,List<Long>>>(triplet.dstId(),new Tuple2<Integer, List<Long>>(sourceVertex._2._1+edge, sourceVertex._2._2))).iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Tuple2<Integer, List<Long>>,Tuple2<Integer, List<Long>>,Tuple2<Integer, List<Long>>> implements Serializable {
        @Override
        public Tuple2<Integer, List<Long>> apply(Tuple2<Integer, List<Long>> o, Tuple2<Integer, List<Long>> o2) {
            if (o._1 < o2._1) {
                return o;
            }
            else {
                return o2;
            }
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();

        List<Tuple2<Object, Tuple2<Integer, List<Long>>>> vertices = Lists.newArrayList(
                new Tuple2<Object,Tuple2<Integer,List<Long>>>(1l,new Tuple2<>(0, Lists.newArrayList())),
                new Tuple2<Object,Tuple2<Integer,List<Long>>>(2l,new Tuple2<>(Integer.MAX_VALUE, Lists.newArrayList())),
                new Tuple2<Object,Tuple2<Integer,List<Long>>>(3l,new Tuple2<>(Integer.MAX_VALUE, Lists.newArrayList())),
                new Tuple2<Object,Tuple2<Integer,List<Long>>>(4l,new Tuple2<>(Integer.MAX_VALUE, Lists.newArrayList())),
                new Tuple2<Object,Tuple2<Integer,List<Long>>>(5l,new Tuple2<>(Integer.MAX_VALUE, Lists.newArrayList())),
                new Tuple2<Object,Tuple2<Integer,List<Long>>>(6l,new Tuple2<>(Integer.MAX_VALUE, Lists.newArrayList()))
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l,2l, 4), // A --> B (4)
                new Edge<Integer>(1l,3l, 2), // A --> C (2)
                new Edge<Integer>(2l,3l, 5), // B --> C (5)
                new Edge<Integer>(2l,4l, 10), // B --> D (10)
                new Edge<Integer>(3l,5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );

        JavaRDD<Tuple2<Object, Tuple2<Integer, List<Long>>>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Tuple2<Integer,List<Long>>,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),new Tuple2<>(1,Lists.newArrayList()), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        ops.pregel(new Tuple2<>(Integer.MAX_VALUE, Lists.newArrayList()),
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new Exercise_3.VProg(),
                new Exercise_3.sendMsg(),
                new Exercise_3.merge(),
                ClassTag$.MODULE$.apply(Tuple2.class))
                .vertices()
                .toJavaRDD()
                // Sorting from the lowest cost to the highest cost:
                .sortBy(f -> ((Tuple2<Object,Tuple2<Integer, List<Long>>>) f)._2._1, true, 0)
                .foreach(v -> {
                    Tuple2<Object,Tuple2<Integer, List<Long>>> vertex = (Tuple2<Object,Tuple2<Integer, List<Long>>>)v;
                    List<String> path = vertex._2._2.stream().map(t -> labels.get(t)).collect(Collectors.toList());
                    System.out.println("Minimum path to get from " + path.get(0) + " to " + path.get(path.size()-1) + " is " + path + " with cost " + vertex._2._1);
                });
    }
}
