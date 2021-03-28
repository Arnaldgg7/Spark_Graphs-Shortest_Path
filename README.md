### The 2 present implementations showcase the power of Spark Graphs, a Spark Module built on top of GraphX using Pegel as implementation of TLAV (Thinking Like A Vertex) framework to manage distributed graphs.

#### The first one implements the Pregel framework in order to construct a distributed graph in which the vertices interact with each other towards achieving a specific goal. In the present case, such interaction is aimed at exchanging the 'costs' that each edge in the graph is storing, so that we can finally compute the shortest path between 2 vertices in the graph in terms of the existing costs of the edges.

#### Finally, there is a 2nd implementation that, besides providing you the shortest path and its cost between each of the vertices of the graph, it also provides the exact 'path' in terms of vertices you must get through in order to get from one vertex to another one.

These implementations might be applied to a wide variety of real case scenarios, such as traffic routes (in fact, Google Maps implements such a system in real-time) or communication topologies, aimed at detecting overused antennas by telecommunication companies. 


### As a final conclusion, the use of Spark Graphs and its Pregel framework allows you to benefit from the 2 following aspects:

#### - The potentially limitless size of the graph you are storing, since the fact of being distributed enables you to make grow your graph as much as you want over the cluster.

#### - The improvement in the Shortest Path Algorithm implementation with regard to the original implementation that stems from the fact of using the 'triplet' file to store the Graphs. That is, the graph itself is actually stored in 3 files on disk: 1 Vertex File, 1 Edge File and 1 Triplet File (made of the IDs of the 'vertex-vertex-edge' that are connected together within the same file). Therefore, it allows this implementation to decide as to send the message to the adjacent node or not, depending on its actual value, thereby improving the overall throughput of such an expensive, iterative calculation which is the Shortes Path Algorithm, as well as reducing the communication overhead in the cluster overall (main bottleneck of distributed systems).
