package org.janusgraph.kggraph;

import com.google.common.collect.Iterables;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphVertex;
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
            Vertex next = g.V(LongEncoding.decode("81xbbkg123_23_000")).next();
            Iterable<JanusGraphVertex> vertices = getJanusGraph().query().limit(10).vertices();
            vertices.forEach(janusGraphVertex -> {
                Iterable<JanusGraphEdge> edgesOut = janusGraphVertex.query().direction(Direction.OUT).edges();
                Iterable<JanusGraphEdge> edgesIn = janusGraphVertex.query().direction(Direction.IN).edges();
                System.out.println(Iterables.size(edgesOut));
                System.out.println(Iterables.size(edgesIn));
            });
            List<Map<Object, Object>> maps = g.V(LongEncoding.decode("214xbbkg123_13_000")).elementMap().toList();
            List<Map<Object, Object>> out = g.V(LongEncoding.decode("214xbbkg123_13_000")).out().elementMap().toList();
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
