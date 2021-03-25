package org.janusgraph.kggraph;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.vertices.AbstractVertex;
import org.janusgraph.kydsj.serialize.Note;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Iterator;

/**
 * 测试对象注释
 */
public class PropertyPropertiesTest extends AbstractKGgraphTest{
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertyPropertiesTest.class);

    @Test
    public void insertPropertyProperties(){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            String tid="tid002";
            String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                .addV("object_qq")
                .property("name", "我是测试qq111",
                    "startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", "程序导入2222",
                    "geo", Geoshape.point(22.22, 113.1122))
                .property(T.id, tid);
            Vertex qq = qqTraversal.next();
            threadedTx.commit();
        }
    }

    @Test
    public void insertPropertyPropertiesOther(){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            String tid="tid002";
            String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                .addV("object_qq")
                .property("name", "我是测试qq",
                    "startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", "程序导入2222_other",
                    "geo", Geoshape.point(24.22, 114.1122))
                .property(T.id, tid);
            Vertex qq = qqTraversal.next();
            threadedTx.commit();
        }
    }

    @Test
    public void deleteProperty()
    {
        try {
            if (g == null) {
                return;
            }
            LOGGER.info("deleting elements");
            String tid="tid002";
            String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            System.out.println(graphId);
            g.T(tid).properties("name").filter(p->{
                Property<Object> property = p.get();
                String s = property.value().toString();
                if(s.equals("我是测试qq111")){
                    return true;
                }
                return false;
            }).drop().toList();
            g.tx().commit();
        } catch (Exception e) {
            g.tx().rollback();
        }
    }

    /**
     * 根据tid查询顶点对象
     */
    @Test
    public void selectByTid1(){
        String tid="tid002";
        Vertex next = g.T(tid).next();
        Iterator<VertexProperty<Object>> qq_num_properties = next.properties();
        AbstractVertex vertex=(AbstractVertex)next;
        Iterator<Note> notes = vertex.notes();
        if(notes!=null) {
            while (notes.hasNext()){
                Note note=notes.next();
                LOGGER.info(note.toString());
                LOGGER.info("--------------------------------------------");
            }
        }
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
    public void deleteElements() {
        try {
            if (g == null) {
                return;
            }
            LOGGER.info("deleting elements");
            String tid="tid002";
            String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            System.out.println(graphId);
            g.T(tid).drop().toList();
            g.tx().commit();
        } catch (Exception e) {
            g.tx().rollback();
        }
    }


}
