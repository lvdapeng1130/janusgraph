package org.janusgraph.kggraph;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
    public void insertPropertyPropertiesOther1(){
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
                    "dsr", "程序导入2222_other_111",
                    "geo", Geoshape.point(24.22, 114.1122))
                .property(T.id, tid);
            Vertex qq = qqTraversal.next();
            threadedTx.commit();
        }
    }

    @Test
    public void insertPropertyPropertiesOther2(){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            String tid="tid002";
            String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                .addV("object_qq")
                .property(T.id, tid);
            Vertex qq = qqTraversal.next();
            VertexProperty<String> property = qq.property("name", "我是测试qq",
                "startDate", new Date(),
                "endDate", new Date(),
                "dsr", "程序导入2222_other_222","dsr","lcdp","dsr","喂喂喂我认为",
                "geo", Geoshape.point(24.22, 114.1122));
           /* property.property("dsr","dsr1");
            property.property("dsr","dsr2");
            property.property("dsr","dsr3");*/
            threadedTx.commit();
        }
    }

    @Test
    public void insertPropertyPropertiesOther3(){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            String tid="tid002";
            String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            for(int i=0;i<50;i++) {
                GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                    .addV("object_qq")
                    .property("name", "我是测试qq",
                        "startDate", new Date(),
                        "endDate", new Date(),
                        "dsr", "程序导入2222_other_333"+i,
                        "geo", Geoshape.point(24.22, 114.1122))
                    .property(T.id, tid);
                Vertex qq = qqTraversal.next();
            }
            threadedTx.commit();
        }
    }

    @Test
    public void insertPropertyPeropertiesThread() throws ExecutionException, InterruptedException {
        int p=10;
        ExecutorService pool = Executors.newFixedThreadPool(p,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("import-data-%d").build());//定义线程数
        List<Future<Integer>> futures= Lists.newArrayList();
        for(int t=0;t<p;t++) {
            Future<Integer> submit = pool.submit(() -> {
                try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                    .consistencyChecks(true)
                    .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
                    String tid="tid002";
                    for(int i=0;i<3;i++) {
                        GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                            .addV("object_qq")
                            .property("name", "我是测试qq",
                                "startDate", new Date(),
                                "endDate", new Date(),
                                "dsr", Thread.currentThread().getName()+"-->"+i,
                                "geo", Geoshape.point(24.22, 114.1122))
                            .property(T.id, tid);
                        Vertex qq = qqTraversal.next();
                    }
                    threadedTx.commit();
                }
                return 0;
            });
            futures.add(submit);
        }
        long total=0;
        for(Future<Integer> future:futures){
            total+=future.get();
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
                if(s.equals("我是测试qq")){
                    return true;
                }
                return false;
            }).drop().toList();
            g.tx().commit();
        } catch (Exception e) {
            g.tx().rollback();
        }
    }

    @Test
    public void deleteDsrProperty()
    {
        try {
            if (g == null) {
                return;
            }
            LOGGER.info("deleting elements");
            String tid="tid002";
            String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            System.out.println(graphId);
            List<? extends Property<Object>> name = g.T(tid).properties("name").toList();
            for(Property<Object> property:name){
                VertexProperty vertexProperty=(VertexProperty) property;
                Iterator<Property> propertyIterator = vertexProperty.properties("dsr");
                while (propertyIterator.hasNext()){
                    Property dsrProperty=propertyIterator.next();
                    Object value = dsrProperty.value();
                    System.out.println(value);
                   /* if("程序导入2222_other_222".equals(value))
                    {*/
                        dsrProperty.remove();
                    //}

                }
            }
            g.tx().commit();
        } catch (Exception e) {
            g.tx().rollback();
        }
    }

    @Test
    public void deleteGeoProperty()
    {
        try {
            if (g == null) {
                return;
            }
            LOGGER.info("deleting elements");
            String tid="tid002";
            String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            System.out.println(graphId);
            List<? extends Property<Object>> name = g.T(tid).properties("name").toList();
            for(Property<Object> property:name){
                VertexProperty vertexProperty=(VertexProperty) property;
                Iterator<Property> propertyIterator = vertexProperty.properties("geo");
                while (propertyIterator.hasNext()){
                    Property geoProperty=propertyIterator.next();
                    Object value = geoProperty.value();
                    System.out.println(value);
                    geoProperty.remove();
                }
            }
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
