package org.janusgraph.kggraph;

import com.google.common.base.Stopwatch;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.dsl.KydsjTraversal;
import org.janusgraph.dsl.__;
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
public class SelectedKGgraph2 extends AbstractKGgraphTest3{
    private static final Logger LOGGER = LoggerFactory.getLogger(SelectedKGgraph2.class);

    /**
     * 根据tid查询顶点对象
     */
    @Test
    public void selectByTid1(){
        String tid="tid0014";
        Vertex next = g.T(tid).next();
        System.out.println(next.label());
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
    public void find5floor() {
        String tid = "0aec0be7634d88f0_10_000";
        Stopwatch started = Stopwatch.createStarted();
        KydsjTraversal<Vertex, Vertex> traversal = g.V(tid).repeat(__.both()).times(4).simplePath().limit(10);
        Optional<Vertex> vertexOptional = traversal.tryNext();
        while (vertexOptional.isPresent()) {
            Vertex next = vertexOptional.get();
            System.out.println(next.label() + "--->" + next.id());
            /*Iterator<VertexProperty<Object>> qq_num_properties = next.properties();
            while (qq_num_properties.hasNext()) {
                VertexProperty<Object> vertexProperty = qq_num_properties.next();
                if (vertexProperty.isPresent()) {
                    Object value = vertexProperty.value();
                    System.out.println(vertexProperty.key() + "->" + value);
                    Iterator<Property<Object>> properties = vertexProperty.properties();
                    while (properties.hasNext()) {
                        Property<Object> property = properties.next();
                        if (property.isPresent()) {
                            Object value1 = property.value();
                            System.out.println(property.key() + "<->" + value1);
                        }
                    }
                }
            }*/
            vertexOptional = traversal.tryNext();
        }
        started.stop();
        System.out.println("用时---->" + started.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    public void find5floorPath() {
        String tid = "ce119b4ebff1571b_2_000";
        Stopwatch started = Stopwatch.createStarted();
        //qqqun->qq->qqqun->qq->qqqun
        //eidentity_qqqun->eidentity_qq
        KydsjTraversal<Vertex, Map<String, Object>> traversal = g.V().as("n_0").hasLabel("eidentity_qqqun").has("eidentity_qqqun", "48798616")
            .outE("link_member").inV().hasLabel("eidentity_qq")
            .inE("link_member").outV().hasLabel("eidentity_qqqun")
            .outE("link_member").inV().hasLabel("eidentity_qq")
            .inE("link_member").outV().hasLabel("eidentity_qqqun")
            .simplePath().path().from("n_0").as("p").project("p").by(__.identity()).limit(100).project("p");
        System.out.println(traversal.toList().size());

      /*
        KydsjTraversal<Vertex, Path> traversal = g.V().as("n_0").hasLabel("eidentity_qqqun").has("eidentity_qqqun","48798616")
            .outE("link_member").inV().hasLabel("eidentity_qq")
            .inE("link_member").outV().hasLabel("eidentity_qqqun")
            .outE("link_member").inV().hasLabel("eidentity_qq")
            .inE("link_member").outV().hasLabel("eidentity_qqqun")
            .simplePath().path().from("n_0").as("p").project("p").by(__.identity()).limit(100).project("p");
        Optional<Path> pathOptional = traversal.tryNext();
        while (pathOptional.isPresent()) {
            Path path = pathOptional.get();
            System.out.println(path);
            pathOptional = traversal.tryNext();
        }*/
        started.stop();
        System.out.println("用时---->" + started.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    public void test(){
        KydsjTraversal<Vertex, Long> count = g.V("89cd667c660f1e8a_1_000", "e62a891d3748a86a_30_000", "e62a88c1a0070d7f_7_000", "13c43b215b40ac57_18_000").bothE().count();
        List<Long> longs = count.toList();
        System.out.println(22);
    }
}
