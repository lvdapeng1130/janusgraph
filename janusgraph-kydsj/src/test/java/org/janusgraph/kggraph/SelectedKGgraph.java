package org.janusgraph.kggraph;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.util.encoding.LongEncoding;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author: ldp
 * @time: 2021/1/18 15:27
 * @jira:
 */
public class SelectedKGgraph extends AbstractKGgraphTest{
    private static final Logger LOGGER = LoggerFactory.getLogger(ManageDataTest.class);
    @Test
    public void readElements() {
        try {
            if (g == null) {
                return;
            }
            //Vertex next = g.V(LongEncoding.decode("qqqun$333_25_000")).next();
            /*Iterable<JanusGraphVertex> vertices = getJanusGraph().query().limit(10).vertices();
            vertices.forEach(janusGraphVertex -> {
                Iterable<JanusGraphEdge> edgesOut = janusGraphVertex.query().direction(Direction.OUT).edges();
                Iterable<JanusGraphEdge> edgesIn = janusGraphVertex.query().direction(Direction.IN).edges();
                System.out.println(Iterables.size(edgesOut));
                System.out.println(Iterables.size(edgesIn));
            });*/
            Vertex next = g.V("qq$crAdrvnl4WM_18_000").next();
            Iterator<VertexProperty<Object>> qq_num_properties = next.properties("qq_num");
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
            List<Map<Object, Object>> maps = g.V(LongEncoding.decode("qqqun$333_25_000")).elementMap().toList();
            List<Map<Object, Object>> out = g.V("qqqun$333_25_000").out().limit(2).in().elementMap().toList();
            List<Map<Object, Object>> in = g.V(LongEncoding.decode("990xbbkg123000")).in().elementMap().toList();
            List<Map<Object, Object>> others = g.V(LongEncoding.decode("990xbbkg123000")).both().elementMap().toList();
            List<Vertex> vertices4 = g.V().hasLabel("object_qqqun").has("qqqun_num", "308").limit(1).toList();

        } finally {
            g.tx().rollback();
        }
    }
}
