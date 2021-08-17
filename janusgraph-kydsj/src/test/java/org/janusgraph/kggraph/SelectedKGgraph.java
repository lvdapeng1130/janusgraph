package org.janusgraph.kggraph;

import com.google.common.base.Stopwatch;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.dsl.__;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.util.encoding.LongEncoding;
import org.janusgraph.util.system.DefaultKeywordField;
import org.janusgraph.util.system.DefaultTextField;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * @author: ldp
 * @time: 2021/1/18 15:27
 * @jira:
 */
public class SelectedKGgraph extends AbstractKGgraphTest{
    private static final Logger LOGGER = LoggerFactory.getLogger(SelectedKGgraph.class);

    /**
     * 根据tid查询顶点对象
     */
    @Test
    public void selectByTid1(){
        String tid="tid004";
        Vertex next = g.T(tid).next();
        Iterator<VertexProperty<Object>> qq_num_properties = next.properties();
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
                        System.out.println(property.key()+"<->"+value1);
                    }
                }
            }
        }
    }
    /**
     * 根据tid查询顶点对象
     */
    @Test
    public void selectByTid2(){
        String tid="tid003";
        String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
        Vertex next = g.V(graphId).next();
        Iterator<VertexProperty<Object>> qq_num_properties = next.properties();
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
                        System.out.println(property.key()+"<->"+value1);
                    }
                }
            }
        }
    }

    @Test
    public void selectLink(){
        Optional<Vertex> vertex = g.T("686").tryNext();
        System.out.println("开始.....");
        Stopwatch started = Stopwatch.createStarted();
        Long next = g.T("686", "354", "200", "202", "232", "890", "12", "233", "190", "122").out().count().next();
        Map<Object, Object> label = g.T("686", "354", "200", "202", "232", "890", "12", "233", "190", "122").out().group().by("age1").by(__.count()).next();
        Map<Object, Object> label2 = g.T("686", "354", "200", "202", "232", "890", "12", "233", "190", "122").out().group().by("age1").by(__.count()).next();
        started.stop();
        LOGGER.info("条数："+label.size()+"用时："+started.elapsed(TimeUnit.MILLISECONDS));
    }
    @Test
    public void count(){
        //Long next = g.V().hasLabel("object_qq").has(DefaultTextField.TITLE.getName(), "我是测试标签").count().next();
        Long next = g.V().hasLabel("object_qq").has(DefaultTextField.TITLE.getName(),"我是测试标签").count().next();
        LOGGER.info("数量："+next);
    }
    @Test
    public void selectBothLink(){
        Optional<Vertex> vertex = g.V().hasLabel("object_qq").has(DefaultTextField.TITLE.getName(), "我是测试标签").both().tryNext();
        if(vertex.isPresent()){
            Vertex next = vertex.get();
            Iterator<VertexProperty<Object>> qq_num_properties = next.properties();
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
                            System.out.println(property.key()+"<->"+value1);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void selectLinkId(){
        Optional<Edge> optionalEdge = g.E("5120016_28-tid003_28_000-416010101-tid004_27_000").tryNext();
        if(optionalEdge.isPresent()){
            Edge edge = optionalEdge.get();
            Iterator<Property<Object>> edgeProperties = edge.properties();
            while (edgeProperties.hasNext()){
                Property<Object> edgeProperty = edgeProperties.next();
                if(edgeProperty.isPresent()){
                    Object value = edgeProperty.value();
                    System.out.println(edgeProperty.key()+"->"+value);
                }
            }
        }
    }



    @Test
    public void selectTid() {
        Optional<Vertex> tid003 = g.V().has(DefaultKeywordField.TID.getName(), "tid003").tryNext();
        if(tid003.isPresent()){
            Vertex next = tid003.get();
            Iterator<VertexProperty<Object>> qq_num_properties = next.properties();
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
                            System.out.println(property.key()+"<->"+value1);
                        }
                    }
                }
            }
        }
    }

    /**
     * mixed索引必须指定label才能使用索引
     */
    @Test
    public void selectObjLabel() {
        Optional<Vertex> tid003 = g.V().hasLabel("object_qq").has(DefaultTextField.TITLE.getName(),"我是测试标签").tryNext();
        if(tid003.isPresent()){
            Vertex next = tid003.get();
            Iterator<VertexProperty<Object>> qq_num_properties = next.properties();
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
                            System.out.println(property.key()+"<->"+value1);
                        }
                    }
                }
            }
        }
    }

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
            Vertex next = g.V("686").next();
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
                            System.out.println(property.key()+"<->"+value1);
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
