package org.janusgraph.kggraph;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.*;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.dsl.KydsjTraversalSource;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.util.MD5Util;
import org.janusgraph.graphdb.vertices.AbstractVertex;
import org.janusgraph.util.system.DefaultFields;
import org.janusgraph.util.system.DefaultKeywordField;
import org.janusgraph.util.system.DefaultTextField;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author: ldp
 * @time: 2021/1/18 14:46
 * @jira:
 */
@Slf4j
public class ManageDataNewTest extends AbstractKGgraphTest{
    private static final Logger LOGGER = LoggerFactory.getLogger(ManageDataNewTest.class);
    private String id="tid001_30_000";
    @Test
    public void insertTestDate(){
        for (int i = 0; i < 20; i++) {
            for (int j = 0; j < 3; j++) {
                insertDate("tid00"+i, "dsr" + j);
            }
        }
    }

    @Test
    public void insertTestDate1(){
        String tid="tid001";
        String dsr="dsr1";
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String ds="2022-10-10 12:12:12";
            Date date = sdf.parse(ds);
            GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                .addV("object_qq")
                .property("time",date,"dsr",dsr)
                .property("name",ds,"dsr",dsr,"startDate", new Date(),
                    "endDate", new Date(),
                    "geo", Geoshape.point(22.22, 113.1122))
                .property("qq_num","99999","dsr","dsr1")
                .property("db",123,"dsr","dsr2")
                .property("db",23,"dsr","dsr1")
                .property(DefaultTextField.TITLE.getName(),"我是测试标签")
                .property(T.id, tid);
            AbstractVertex qq = (AbstractVertex)qqTraversal.next();
            VertexProperty<Object> property = qq.trsProperty("db",12);
            for(int i=0;i<3;i++){
                property.property("dsr","dsr"+i);
            }
            threadedTx.commit();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 构建测试数据：20条数据每条数据100多个属性值，每个属性值3个dsr
     */
    @Test
    public void insertTestData(){
        int thread=Runtime.getRuntime().availableProcessors();
        final int preThreadSize=10;
        final int everyThreadSize=10000;
        try {
            Stopwatch started = Stopwatch.createStarted();
            ExecutorService pool = Executors.newFixedThreadPool(thread,
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("t-%d").build());//定义线程数
            List<Future<Integer>> futures= Lists.newArrayList();
            for(int t=0;t<thread;t++) {
                Future<Integer> submit = pool.submit(() -> {
                    StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                        .consistencyChecks(true)
                        .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start();
                    long begin=System.currentTimeMillis();
                    int totalSize=0;
                    for (int i = 0; i < everyThreadSize; i++) {
                        String tid=String.format("%s-tid00%s",Thread.currentThread().getName(),i);
                        insertData(threadedTx,tid,"测试");
                        totalSize++;
                        if(i!=0&&i%preThreadSize==0){
                            threadedTx.commit();
                            threadedTx=(StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                                .consistencyChecks(true)
                                .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start();
                            long end=System.currentTimeMillis();
                            LOGGER.info(String.format("线程%s,批次大小%s用时%s,共处理了->%s条",Thread.currentThread().getName(),preThreadSize,(end-begin),totalSize));
                            begin=end;
                        }
                    }
                    if(threadedTx.isOpen()){
                        threadedTx.commit();
                    }
                    return totalSize;
                });
                futures.add(submit);
            }
            long total=0;
            for(Future<Integer> future:futures){
                total+=future.get();
            }
            started.stop();
            LOGGER.info(String.format("所有线程,一共处理了->%s条,用时%s", total,started.elapsed(TimeUnit.MILLISECONDS)));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            g.tx().rollback();
        }
    }
    private void insertData(StandardJanusGraphTx threadedTx ,String tid,String dsr) throws ParseException {
        String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String ds="2022-10-10 12:12:12";
        Date date = sdf.parse(ds);
        GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
            .addV("object_qq")
            .property("time",date,"dsr",dsr)
            .property("name",ds,"dsr",dsr,"startDate", new Date(),
                "endDate", new Date(),
                "geo", Geoshape.point(22.22, 113.1122))
            .property("qq_num","99999","dsr","dsr1")
            .property("db",123,"dsr","dsr2")
            .property("db",23,"dsr","dsr1")
            .property(DefaultTextField.TITLE.getName(),"我是测试标签")
            .property(T.id, tid);
        AbstractVertex qq = (AbstractVertex)qqTraversal.next();
        for(int i=0;i<100;i++){
            VertexProperty<Object> property=qq.trsProperty("name"+i,"我是测试属性name"+i,"startDate", new Date(),
                "endDate", new Date(),
                "geo", Geoshape.point(22.22, 113.1122));
            for(int j=0;j<3;j++){
                property.property("dsr","dsr"+j);
            }
        }

    }
    private void insertDate(String tid,String dsr){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                .consistencyChecks(true)
                .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String ds="2022-10-10 12:12:12";
            Date date = sdf.parse(ds);
            GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                    .addV("object_qq")
                    .property("time",date,"dsr",dsr)
                    .property("name",ds,"dsr",dsr,"startDate", new Date(),
                            "endDate", new Date(),
                            "geo", Geoshape.point(22.22, 113.1122))
                    .property("qq_num","99999","dsr","dsr1")
                    .property("db",123,"dsr","dsr2")
                    .property("db",23,"dsr","dsr1")
                    .property(DefaultTextField.TITLE.getName(),"我是测试标签")
                    .property(T.id, tid);
            for(int i=0;i<100;i++){
                qqTraversal.property("name"+i,"我是测试属性name"+i,"dsr",dsr,"startDate", new Date(),
                        "endDate", new Date(),
                        "geo", Geoshape.point(22.22, 113.1122));
            }
            Vertex qq = qqTraversal.next();
            threadedTx.commit();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void deletePropertyDsr(){
        removeDsr("dsr1");
        removeDsr("dsr2");
        removeDsr("dsr3");
    }

    @Test
    public void batchDeletePropertyDsr(){
        for(int i=0;i<3;i++) {
            removeBatchDsr("dsr"+i);
        }
    }

    @Test
    public void selectByFull(){
        String id="tid001_30_000";
        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.buildTransaction().consistencyChecks(true)
                .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start();
        KydsjTraversalSource g = tx.traversal(KydsjTraversalSource.class);
        Stopwatch started = Stopwatch.createStarted();
        Optional<Vertex> vertex = g.V(id).tryNext();
        if(vertex.isPresent()) {
            Vertex next = vertex.get();
            Iterator<VertexProperty<Object>> qq_num_properties = next.properties();
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
            }
        }
        started.stop();
        log.info("用时："+started.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    public void removeProperty(){
        removeProperty("我是测试属性name23");
        removeProperty("我是测试属性name28");
        removeProperty("我是测试属性name66");
    }
    /**
     * 删除属性
     */
    private void removeProperty(Object delValue){
        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.buildTransaction().consistencyChecks(false)
                .checkInternalVertexExistence(false).checkExternalVertexExistence(true).start();
        KydsjTraversalSource g = tx.traversal(KydsjTraversalSource.class);
        Stopwatch started = Stopwatch.createStarted();
        boolean change=false;
        Optional<Vertex> vertex = g.V(id).tryNext();
        if(vertex.isPresent()) {
            Vertex next = vertex.get();
            Iterator<VertexProperty<Object>> properties = next.properties();
            while (properties.hasNext()) {
                VertexProperty<Object> vertexProperty = properties.next();
                if (vertexProperty.isPresent()) {
                    Object value = vertexProperty.value();
                    if(value.equals(delValue)){
                        vertexProperty.remove();
                        change=true;
                    }
                }
            }
        }
        if(change) {
            tx.commit();
        }else{
            tx.rollback();
        }
        started.stop();
        log.info("用时："+started.elapsed(TimeUnit.MILLISECONDS));
    }



    /**
     * 删除属性的dsr
     */
    private void removeDsr(String delDsr){
        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.buildTransaction().consistencyChecks(false)
                .checkInternalVertexExistence(false).checkExternalVertexExistence(true).start();
        KydsjTraversalSource g = tx.traversal(KydsjTraversalSource.class);
        Stopwatch started = Stopwatch.createStarted();
        boolean change=false;
        Optional<Vertex> vertex = g.V(id).tryNext();
        if(vertex.isPresent()) {
            Vertex next = vertex.get();
            Iterator<VertexProperty<Object>> properties = next.properties();
            while (properties.hasNext()) {
                VertexProperty<Object> vertexProperty = properties.next();
                if (vertexProperty.isPresent()) {
                    Object value = vertexProperty.value();
                    System.out.println(vertexProperty.key() + "->" + value);
                    Iterator<Property<Object>> dsrProperties = vertexProperty.properties("dsr");
                    while (dsrProperties.hasNext()) {
                        Property<Object> dsr = dsrProperties.next();
                        if (dsr.isPresent()) {
                            Object value1 = dsr.value();
                            System.out.println(dsr.key() + "<->" + value1);
                            if(value1.equals(delDsr)){
                                dsr.remove();
                                change=true;
                            }
                        }
                    }
                }
            }
        }
        if(change) {
            tx.commit();
        }else{
            tx.rollback();
        }
        started.stop();
        log.info("删除"+delDsr+"用时："+started.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    public void batchDeleteDsr(){
        for(int i=0;i<3;i++) {
            batchDeleteDsr("dsr"+i);
        }
    }

    private void batchDeleteDsr(String delDsr){
        int thread=Runtime.getRuntime().availableProcessors();
        final int preThreadSize=10;
        final int everyThreadSize=10000;
        try {
            Stopwatch started = Stopwatch.createStarted();
            ExecutorService pool = Executors.newFixedThreadPool(thread,
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("t-%d").build());//定义线程数
            List<Future<Integer>> futures= Lists.newArrayList();
            for(int t=0;t<thread;t++) {
                Future<Integer> submit = pool.submit(() -> {
                    StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                        .consistencyChecks(true)
                        .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start();
                    long begin=System.currentTimeMillis();
                    KydsjTraversalSource g = threadedTx.traversal(KydsjTraversalSource.class);
                    int totalSize=0;
                    for (int i = 0; i < everyThreadSize; i++) {
                        String tid=String.format("%s-tid00%s",Thread.currentThread().getName(),i);
                        Optional<Vertex> vertex = g.T(tid).tryNext();
                        if (vertex.isPresent()) {
                            Vertex next = vertex.get();
                            Iterator<VertexProperty<Object>> properties = next.properties();
                            while (properties.hasNext()) {
                                VertexProperty<Object> vertexProperty = properties.next();
                                if (vertexProperty.isPresent()) {
                                    Object value = vertexProperty.value();
                                    Iterator<Property<Object>> dsrProperties = vertexProperty.properties("dsr");
                                    while (dsrProperties.hasNext()) {
                                        Property<Object> dsr = dsrProperties.next();
                                        if (dsr.isPresent()) {
                                            Object value1 = dsr.value();
                                            if (value1.equals(delDsr)) {
                                                dsr.remove();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        totalSize++;
                        if(i!=0&&i%preThreadSize==0){
                            threadedTx.commit();
                            threadedTx=(StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                                .consistencyChecks(true)
                                .checkInternalVertexExistence(true).checkExternalVertexExistence(true)
                                .start();
                            g = threadedTx.traversal(KydsjTraversalSource.class);
                            long end=System.currentTimeMillis();
                            LOGGER.info(String.format("线程%s,批次大小%s用时%s,共处理了->%s条",Thread.currentThread().getName(),preThreadSize,(end-begin),totalSize));
                            begin=end;
                        }
                    }
                    if(threadedTx.isOpen()){
                        threadedTx.commit();
                    }
                    return totalSize;
                });
                futures.add(submit);
            }
            long total=0;
            for(Future<Integer> future:futures){
                total+=future.get();
            }
            started.stop();
            LOGGER.info(String.format("所有线程,一共处理了->%s条,用时%s", total,started.elapsed(TimeUnit.MILLISECONDS)));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            g.tx().rollback();
        }
    }

    /**
     * 删除属性的dsr
     */
    public void removeBatchDsr(String delDsr){
        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.buildTransaction().consistencyChecks(false)
                .checkInternalVertexExistence(false).checkExternalVertexExistence(true).start();
        KydsjTraversalSource g = tx.traversal(KydsjTraversalSource.class);
        Stopwatch started = Stopwatch.createStarted();
        boolean change=false;
        for(int i=0;i<20;i++) {
            Optional<Vertex> vertex = g.T("tid00"+i).tryNext();
            if (vertex.isPresent()) {
                Vertex next = vertex.get();
                Iterator<VertexProperty<Object>> properties = next.properties();
                while (properties.hasNext()) {
                    VertexProperty<Object> vertexProperty = properties.next();
                    if (vertexProperty.isPresent()) {
                        Object value = vertexProperty.value();
                        Iterator<Property<Object>> dsrProperties = vertexProperty.properties("dsr");
                        while (dsrProperties.hasNext()) {
                            Property<Object> dsr = dsrProperties.next();
                            if (dsr.isPresent()) {
                                Object value1 = dsr.value();
                                if (value1.equals(delDsr)) {
                                    dsr.remove();
                                    change = true;
                                }
                            }
                        }
                    }
                }
            }
        }
        if(change) {
            tx.commit();
        }else{
            tx.rollback();
        }
        started.stop();
        log.info("删除"+delDsr+"用时："+started.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    public void batchDeleteAll(){
        int thread=Runtime.getRuntime().availableProcessors();
        final int preThreadSize=30;
        final int everyThreadSize=10000;
        try {
            Stopwatch started = Stopwatch.createStarted();
            ExecutorService pool = Executors.newFixedThreadPool(thread,
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("t-%d").build());//定义线程数
            List<Future<Integer>> futures= Lists.newArrayList();
            for(int t=0;t<thread;t++) {
                Future<Integer> submit = pool.submit(() -> {
                    StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                        .consistencyChecks(true)
                        .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start();
                    long begin=System.currentTimeMillis();
                    KydsjTraversalSource g = threadedTx.traversal(KydsjTraversalSource.class);
                    int totalSize=0;
                    for (int i = 0; i < everyThreadSize; i++) {
                        String tid=String.format("%s-tid00%s",Thread.currentThread().getName(),i);
                        Optional<Vertex> vertex = g.T(tid).tryNext();
                        if (vertex.isPresent()) {
                            Vertex next = vertex.get();
                            next.remove();
                        }
                        totalSize++;
                        if(i!=0&&i%preThreadSize==0){
                            threadedTx.commit();
                            threadedTx=(StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                                .consistencyChecks(true)
                                .checkInternalVertexExistence(true).checkExternalVertexExistence(true)
                                .start();
                            g = threadedTx.traversal(KydsjTraversalSource.class);
                            long end=System.currentTimeMillis();
                            LOGGER.info(String.format("线程%s,批次大小%s用时%s,共处理了->%s条",Thread.currentThread().getName(),preThreadSize,(end-begin),totalSize));
                            begin=end;
                        }
                    }
                    if(threadedTx.isOpen()){
                        threadedTx.commit();
                    }
                    return totalSize;
                });
                futures.add(submit);
            }
            long total=0;
            for(Future<Integer> future:futures){
                total+=future.get();
            }
            started.stop();
            LOGGER.info(String.format("所有线程,一共处理了->%s条,用时%s", total,started.elapsed(TimeUnit.MILLISECONDS)));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            g.tx().rollback();
        }
    }

    @Test
    public void deleteAll(){
        try {
            if (g == null) {
                return;
            }
            LOGGER.info("deleting elements");
            Stopwatch started = Stopwatch.createStarted();
            for(int i=0;i<20;i++) {
                List<Vertex> vertices = g.T("tid00"+i).toList();
                for (Vertex vertex : vertices) {
                    vertex.remove();
                }
                g.tx().commit();
            }
            started.stop();
            long elapsed = started.elapsed(TimeUnit.MILLISECONDS);
            log.info("总用时："+elapsed+",平均："+elapsed/20d);
        } catch (Exception e) {
            g.tx().rollback();
        }
    }

    @Test
    public void delete1(){
        try {
            if (g == null) {
                return;
            }
            LOGGER.info("deleting elements");
            Stopwatch started = Stopwatch.createStarted();
            List<Vertex> vertices = g.V(id).toList();
            for(Vertex vertex:vertices){
                vertex.remove();
            }
            g.tx().commit();
            started.stop();
            log.info("用时："+started.elapsed(TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            g.tx().rollback();
        }
    }

    @Test
    public void delete2(){
        try {
            if (g == null) {
                return;
            }
            LOGGER.info("deleting elements");
            g.V(id).dropExpand().iterate();
            g.tx().commit();
        } catch (Exception e) {
            g.tx().rollback();
        }
    }

}
