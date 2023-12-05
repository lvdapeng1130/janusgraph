package org.janusgraph.guava;

import com.google.common.graph.ElementOrder;
import com.google.common.graph.MutableNetwork;
import com.google.common.graph.NetworkBuilder;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.dsl.KydsjTraversal;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;

/**
 * @author: ldp
 * @time: 2021/1/18 15:27
 * @jira:
 */
public class SelectedKGgraph extends AbstractKGgraphTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SelectedKGgraph.class);

    /**
     * 遍历图库构建内存图库
     */
    @Test
    public void buildGuavaGraph(){
        MutableNetwork<String, String> network = NetworkBuilder.directed() //有向网
            .allowsParallelEdges(true) //允许并行边
            .allowsSelfLoops(true) //允许自环
            .nodeOrder(ElementOrder.<String>insertion()) //节点顺序
            .edgeOrder(ElementOrder.<String>insertion()) //边顺序
            .expectedNodeCount(10000) //期望节点数
            .expectedEdgeCount(10000) //期望边数
            .build();
        KydsjTraversal<Vertex, Vertex> traversal = g.V();
        Optional<Vertex> vertexOptional = traversal.tryNext();
        long counter=0;
        while (vertexOptional.isPresent()){
            counter++;
            Vertex vertex = vertexOptional.get();
            Iterator<Edge> edges = vertex.edges(Direction.OUT);
            while (edges.hasNext()){
                Edge edge =edges.next();
                String inID = edge.inVertex().id().toString();
                String outID = edge.outVertex().id().toString();
                String linkId=edge.id().toString();
                network.addEdge(outID, inID, linkId);
            }
            vertexOptional=traversal.tryNext();
            if(counter%1000==0){
                System.out.println(String.format("已经读取数据%s条",counter));
            }
        }
        System.out.println(String.format("nodes->%s,edges->%s",network.nodes().size(),network.edges().size()));
    }
    @Test
    public void buildGuavaGraph1(){
        MutableNetwork<String, String> network = NetworkBuilder.directed() //有向网
            .allowsParallelEdges(true) //允许并行边
            .allowsSelfLoops(true) //允许自环
            .nodeOrder(ElementOrder.<String>insertion()) //节点顺序
            .edgeOrder(ElementOrder.<String>insertion()) //边顺序
            .expectedNodeCount(10000) //期望节点数
            .expectedEdgeCount(10000) //期望边数
            .build();
        long counter=0;
        for(int i=0;i<100000000;i++){
            String outID= UUID.randomUUID().toString();
            String inID=UUID.randomUUID().toString();
            String linkId=UUID.randomUUID().toString();
            network.addEdge(outID, inID, linkId);
            counter++;
            if(counter%1000==0){
                System.out.println(String.format("已经读取数据%s条",counter));
            }
        }
        System.out.println(String.format("nodes->%s,edges->%s",network.nodes().size(),network.edges().size()));
    }

}
