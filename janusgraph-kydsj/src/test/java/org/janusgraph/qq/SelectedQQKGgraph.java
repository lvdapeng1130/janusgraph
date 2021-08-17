package org.janusgraph.qq;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.kggraph.AbstractKGgraphTest;
import org.janusgraph.util.encoding.LongEncoding;
import org.janusgraph.util.system.ConfigurationUtil;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author: ldp
 * @time: 2021/1/18 15:27
 * @jira:
 */
public class SelectedQQKGgraph extends AbstractKGgraphTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SelectedQQKGgraph.class);
    private String mixedIndexConfigName="search";
    protected Configuration conf;
    protected JanusGraph graph;
    protected GraphTraversalSource g;
    @Before
    public void startHBase() throws IOException, ConfigurationException {
        LOGGER.info("opening graph");
        conf = ConfigurationUtil.loadPropertiesConfig("D:\\github\\janusgraph\\janusgraph-kydsj\\src\\test\\resources\\trsgraph-hbase-es-test-qq.properties");
        graph = JanusGraphFactory.open(conf);
        g = graph.traversal();
    }
    protected JanusGraph getJanusGraph() {
        return (JanusGraph) graph;
    }
    @Test
    public void readElements() {
        try {
            if (g == null) {
                return;
            }
            //List<Map<Object, Object>> maps1 = getJanusGraph().traversal().E().limit(1).elementMap().toList();
            //List<Map<Object, Object>> maps1 = g.V("1000e35f7fc1fd1f0642271347d3e45d_23_000").outE().elementMap().toList();
            //List<Map<Object, Object>> maps2 = g.V("27025fde4c1903176ec540c2ab705fba_15_000").inE().elementMap().toList();
            List<Map<Object, Object>> maps3 = g.E("371616_23-1000e35f7fc1fd1f0642271347d3e45d_23_000-400010101-27025fde4c1903176ec540c2ab705fba_15_000").elementMap().toList();
            Optional<Edge> optionalEdge=getJanusGraph().traversal().E().hasLabel("link_member")
                .has("linktid","d03752fa84c7b84edde874e8fe4a2b49").tryNext();
            Vertex next = g.V(LongEncoding.decode("qqqun$333_25_000")).next();
            /*Iterable<JanusGraphVertex> vertices = getJanusGraph().query().limit(10).vertices();
            vertices.forEach(janusGraphVertex -> {
                Iterable<JanusGraphEdge> edgesOut = janusGraphVertex.query().direction(Direction.OUT).edges();
                Iterable<JanusGraphEdge> edgesIn = janusGraphVertex.query().direction(Direction.IN).edges();
                System.out.println(Iterables.size(edgesOut));
                System.out.println(Iterables.size(edgesIn));
            });*/
            List<Map<Object, Object>> maps = g.V(LongEncoding.decode("qqqun$333_25_000")).elementMap().toList();
            List<Map<Object, Object>> out = g.V("qqqun$333_25_000").out().limit(2).in().elementMap().toList();
            List<Map<Object, Object>> in = g.V(LongEncoding.decode("990xbbkg123000")).in().elementMap().toList();
            List<Map<Object, Object>> others = g.V(LongEncoding.decode("990xbbkg123000")).both().elementMap().toList();
            Iterator<VertexProperty<Object>> qq_num_properties = next.properties("name");
            while (qq_num_properties.hasNext()){
                VertexProperty<Object> vertexProperty = qq_num_properties.next();
                if(vertexProperty.isPresent()){
                    Object value = vertexProperty.value();
                    System.out.println(vertexProperty.key()+"->"+value);
                    Iterator<Property<Object>> properties = vertexProperty.properties();
                    while (properties.hasNext()){
                        Property<Object> property = properties.next();
                        if(property.isPresent()){
                            Object value1 = property.value();
                            System.out.println(property.key()+","+value1);
                        }
                    }
                }
            }
            List<Vertex> vertices4 = g.V().hasLabel("object_qqqun").has("qqqun_num", "308").limit(1).toList();

        } finally {
            g.tx().rollback();
        }
    }
}
