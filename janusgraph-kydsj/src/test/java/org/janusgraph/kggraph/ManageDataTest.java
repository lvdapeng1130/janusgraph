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
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.dsl.KydsjTraversalSource;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.util.MD5Util;
import org.janusgraph.graphdb.vertices.CacheVertex;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
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
public class ManageDataTest extends AbstractKGgraphTest{
    private static final Logger LOGGER = LoggerFactory.getLogger(ManageDataTest.class);

    @Test
    public void test(){
        String str="Aa";
        System.out.println(str.hashCode());
        String md8 = MD5Util.getMD8(str);
        String md7 = MD5Util.getMD8("BB");
        System.out.println(md8+":"+md7);
        for(int i=0;i<100;i++){
            if(str.hashCode()!="BB".hashCode()){
                System.out.println(false);
            }
        }
    }

    @Test
    public void insertAutoIdData(){
        createElements(true,10,50000);
    }

    @Test
    public void insertTidIdData(){
        String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId("tid000001");
        System.out.println(graphId);
        createElements(false,10,1000);
    }

    @Test
    public void testNum(){
        for (int i=0;i<100;i++){
            long l = System.nanoTime();
            System.out.println(l);
        }
    }

    @Test
    public void tidConvertGraphId(){
        String tid="tid001";
        String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
        LOGGER.info("graphId"+graphId);
    }

    @Test
    public void graphIdConvertTid(){
        String tid="tid001";
        String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
        String newTid = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().fromVertexId(graphId);
        assertTrue(tid.equals(newTid),"一致");
    }

    @Test
    public void insertOSimple(){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            String tid="tid0017";
            String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            /*Optional<Vertex> optionalVertex = threadedTx.traversal().V(graphId).tryNext();
            if(optionalVertex.isPresent()){
                Vertex vertex = optionalVertex.get();
                Iterator<VertexProperty<Object>> name = vertex.properties("name");
                while (name.hasNext()){
                    VertexProperty<Object> next = name.next();
                    next.remove();
                }
                VertexProperty<String> property = vertex.property("name", "测试姓名91");
                property.property("dsr", "dsr61");

            }else {*/
                GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                    .addV("object_qq")
                    .property(DefaultKeywordField.TID.getName(), tid)
                    .property("qq_num", "0001","role","ffsfweofwefwe")
                    .property(DefaultFields.STARTDATE.getName(), new Date())
                    .property(DefaultFields.ENDDATE.getName(), new Date())
                    .property(DefaultFields.UPDATEDATE.getName(), new Date())
                    .property(DefaultTextField.TITLE.getName(), "测试1")
                    .property(T.id, tid);
                Vertex qq = qqTraversal.next();
                VertexProperty<String> property = qq.property("name", "测试姓名21");
                property.property("dsr", "dsr2");
                property.property("role","role在对方水电费");
            //}
            threadedTx.commit();
        }
    }

    /**
     * @see org.janusgraph.graphdb.tinkerpop.JanusGraphBlueprintsTransaction
     * @see org.janusgraph.graphdb.transaction.StandardJanusGraphTx
     */
    @Test
    public void insertSimple(){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(false)
            .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
            for(int i=0;i<100;i++) {
                String tid = "tid00614"+i;
                String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
                GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                    .addV("object_qq")
                    .property(DefaultTextField.TITLE.getName(), "我是测试标签")
                    .property(T.id, tid);
                Vertex qq = qqTraversal.next();
            }
            threadedTx.commit();
        }
    }

    @Test
    public void testString(){
        String s1=new String("test");
        String s2=new String("test");
        System.out.println(s1.intern()==s2.intern());
        String s3="test";
        System.out.println(s1.intern()==s3.intern());
    }

    @Test
    public void insertPerson(){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            String tid="tid002";
            String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                .addV("person")
                /*.property("person_gj", "广州",
                    "startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", "程序导入12223333",
                    "geo", Geoshape.point(22.22, 113.1122))*/
                .property(DefaultTextField.TITLE.getName(),"我是测试标签")
                //.property("identity_zjhm","测试identity_zjhm的值","dsr","程序导入")
                //.property("person_xm","李四","dsr","程序导入")
                .property(T.id, tid);
            Vertex qq = qqTraversal.next();
            threadedTx.commit();
        }
    }
    @Test
    public void itt() throws IOException {
        String context = FileUtils.readFileToString(new File("D:\\xss\\big_xlsx3.txt"), Charset.forName("UTF-8"));
        System.out.println(context.length());
    }


    @Test
    public void insertContent1() throws IOException {
        //String context= FileUtils.readFileToString(new File("D:\\xss\\activityList_5W.txt"), Charset.forName("UTF-8"));
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            String tid="tid0088";
            String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            Date date=new Date();
            Instant instant = date.toInstant();
            LocalDateTime localDateTime = instant.atZone(ZoneOffset.UTC).toLocalDateTime();
            //Date.from(ZonedDateTime.of(localDateTime, ZoneOffset.UTC));
            //String s = DateFormatUtils.formatUTC(date, "yyyy-MM-dd HH:mm:ss");
            GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                .addV("object_qq")
                .property(DefaultTextField.TITLE.getName(),"我是测试标签")
                .property(DefaultFields.DOCTEXT.getName(),"新测试内容")
                .property(T.id, tid);
            Vertex qq = qqTraversal.next();
            threadedTx.commit();
        }
    }

    @Test
    public void insertContent() throws IOException {
        //String context= FileUtils.readFileToString(new File("D:\\xss\\big_xlsx3.txt"), Charset.forName("UTF-8"));
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            String tid="tid00113";
            String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            Date date=new Date();
            Instant instant = date.toInstant();
            LocalDateTime localDateTime = instant.atZone(ZoneOffset.UTC).toLocalDateTime();
            //Date.from(ZonedDateTime.of(localDateTime, ZoneOffset.UTC));
            //String s = DateFormatUtils.formatUTC(date, "yyyy-MM-dd HH:mm:ss");
            GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                .addV("object_qq")
                .property("name", "我是测试14qq11111111111122222222",
                    "startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", "程序导入12223333",
                    "geo", Geoshape.point(22.22, 113.1122))
                .property(DefaultKeywordField.TID.getName(),tid)
                .property(DefaultTextField.TITLE.getName(),"我是测试标签")
                //.property(DefaultFields.DOCTEXT.getName(),context)
                .property(DefaultFields.UPDATEDATE.getName(),new Date(),"startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", "程序导入12223333",
                    "geo", Geoshape.point(22.22, 113.1122))
                //.property(DefaultFields.GEO.getName(),new Date(),Geoshape.point(23.22, 123.1122))
                .property("qq_num","李四","dsr","程序导入")
                .property(T.id, tid);
            Vertex qq = qqTraversal.next();
            threadedTx.commit();
        }
    }


    @Test
    public void insertSimple6(){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            String tid="tidxxx000";
            GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                .addV("document_news")
                .property(DefaultTextField.TITLE.getName(),"我是测试标签QQ群")
                .property(DefaultFields.UPDATEDATE.getName(),new Date(),"startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", "程序导入12223333",
                    "geo", Geoshape.point(22.22, 113.1122))
                //.property(DefaultFields.GEO.getName(),new Date(),Geoshape.point(23.22, 123.1122))
                .property(T.id, tid);
            Vertex qq = qqTraversal.next();
            threadedTx.commit();
        }
    }

    public void insertSimple3(String tid,String tid2,String uuid){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            //.setSkipIndexes(Sets.newHashSet("object_qqqun"))
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                .addV("object_qq")
                .property(DefaultFields.UPDATEDATE.getName(),new Date(),"startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", "32222",
                    "geo", Geoshape.point(23.22, 113.1122))
                .property(DefaultTextField.TITLE.getName(),"我是测试标签3322")
                .property("qq_num","12321313")
                .property(T.id, tid);
            Vertex qq = qqTraversal.next();
            GraphTraversal<Vertex, Vertex> qqTraversal2 = threadedTx.traversal()
                .addV("object_qqqun")
                .property(DefaultFields.UPDATEDATE.getName(),new Date(),"startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", "11111",
                    "geo", Geoshape.point(24.22, 134.1122))
                .property(DefaultTextField.TITLE.getName(),"我是测试标签2")
                .property(T.id, tid2);
            Vertex object_qqqun = qqTraversal2.next();

            String graphId1 = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            String graphId2 = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid2);
            Date date = new Date();
            Edge next = threadedTx.traversal().V(graphId1).as("a")
                .V(graphId2)
                .addE("link_simple")
                .property(T.id,uuid)
                .property("link_tid", uuid)
                .property("link_type","link_simple")
                .property("left_tid", "left_tid1_new")
                .property("right_tid", "right_tid1_new")
                .property(DefaultFields.LINK_TEXT.getName(), "abcd")
                //.property(DefaultFields.STARTDATE.getName(),date)
                //.property(DefaultFields.ENDDATE.getName(), date)
                .property("dsr", RandomStringUtils.randomAlphabetic(4))
                .to("a").next();
            Object id = next.id();
            threadedTx.commit();
        }
    }

    public void insertSimple4(String tid,String tid2,String uuid,String dsr){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            //.setSkipIndexes(Sets.newHashSet("object_qqqun"))
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String ds="2022-10-10 12:12:12";
            Date date = sdf.parse(ds);
            GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                .addV("object_qq")
                .property("time",date,"dsr",dsr)
                .property(DefaultTextField.TITLE.getName(),"我是object_qq标签3322")
                .property("qq_num","12321313")
                .property(T.id, tid);
            Vertex qq = qqTraversal.next();
            GraphTraversal<Vertex, Vertex> qqTraversal2 = threadedTx.traversal()
                .addV("object_qqqun")
                .property(DefaultFields.UPDATEDATE.getName(),new Date(),"startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", dsr,
                    "geo", Geoshape.point(23.22, 113.1122))
                .property(DefaultTextField.TITLE.getName(),"我是object_qqqun标签2")
                .property(T.id, tid2);
            Vertex object_qqqun = qqTraversal2.next();
            String graphId1 = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            String graphId2 = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid2);
            Edge next = threadedTx.traversal().V(graphId2).as("a")
                .V(graphId1)
                .addE("link_simple")
                .property(T.id,uuid)
                .property("link_tid", uuid)
                .property("link_type","link_simple")
                .property("left_tid", graphId1)
                .property("right_tid", graphId2)
                .property(DefaultFields.LINK_TEXT.getName(), "abcd")
                .property("dsr", dsr)
                .to("a").next();
            Object id = next.id();
            threadedTx.commit();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void insertSimple5(){
        insertSimple4("qq1","qqun1","link1","dsr2");
        //insertSimple3("tid3","tid4","x01x");
        //insertSimple3("tid5","tid6","x02x");
        //insertSimple3("tid7","tid8","x03x");
    }

    @Test
    public void insertSimple4(){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).setIndexMode(true).checkExternalVertexExistence(true).start()) {
            String tid="tid0014";
            GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                .addV("object_qq")
                .property(DefaultTextField.TITLE.getName(),"我是测试标签4")
                .property(DefaultTextField.TITLE.getName(),"我是测试标签5")
                .property("name", "我是测试11111",
                    "startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", "程序导入11111",
                    "geo", Geoshape.point(22.22, 113.1122))
                .property("name", "我是测试2222",
                    "startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", "程序导入22222",
                    "geo", Geoshape.point(23.22, 122.1122))
                .property(T.id, tid);
            Vertex qq = qqTraversal.next();
            threadedTx.commit();
        }
    }

    @Test
    public void insertSimple2(){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            String tid="tid0014";
            String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                .addV("object_qq")
                .property("name", "我是测试14qq22222")
                .property("name", "23333")
                .property("name", "4444444")
                .property(T.id, tid);
            Vertex qq = qqTraversal.next();
            Iterator<VertexProperty<Object>> qq_num_properties = qq.properties();
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
            threadedTx.commit();
        }
    }

    @Test
    public void insertOtherSimple(){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            String tid="tid003";
            String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                .addV("object_qq")
                .property("name", "我是测试22222",
                    "startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", "程序导入33344",
                    "geo", Geoshape.point(22.22, 113.1122))
                .property(DefaultKeywordField.TID.getName(),tid)
                .property(DefaultTextField.TITLE.getName(),"我是测试标签2")
                .property("qq_num","2222","dsr","程序导入2")
                .property(T.id, tid);
            Vertex qq = qqTraversal.next();
            threadedTx.commit();
        }
    }

    @Test
    public void insertEdge(){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            String tid1="tid0013";
            String graphId1 = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid1);
            String tid2="tid0014";
            String graphId2 = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid2);
            String uuid = "dwewsss";
            Edge next = threadedTx.traversal().V(graphId1).as("a")
                .V(graphId2)
                .addE("link_simple")
                .property(T.id,"linkIDTest")
                .property("link_tid", uuid)
                .property("link_type","link_simple")
                .property("left_tid", "left_tid1_new")
                .property("right_tid", "right_tid1_new")
                .property("dsr", RandomStringUtils.randomAlphabetic(4))
                .to("a").next();
            Object id = next.id();
            threadedTx.commit();
        }
    }

    @Test
    public void insertEdgeOther(){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            String tid="tid001";
            GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                .addV("object_qq")
                .property("name", "我是测试qq1",
                    "startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", "程序导入",
                    "geo", Geoshape.point(22.22, 113.1122))
                .property(DefaultKeywordField.TID.getName(),tid)
                .property(DefaultTextField.TITLE.getName(),"我是测试标签1")
                .property("qq_num","111111","dsr","程序导入")
                .property(T.id, tid);
            String tid1="tid002";
            GraphTraversal<Vertex, Vertex> qqTraversal1 = threadedTx.traversal()
                .addV("object_qq")
                .property("name", "我是测试qq2",
                    "startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", "程序导入",
                    "geo", Geoshape.point(22.22, 113.1122))
                .property(DefaultKeywordField.TID.getName(),tid)
                .property(DefaultTextField.TITLE.getName(),"我是测试标签2")
                .property("qq_num","222222","dsr","程序导入")
                .property(T.id, tid1);
            Vertex qq1 = qqTraversal.next();
            Vertex qq2 = qqTraversal1.next();
            String uuid = "link1";
            Edge next = threadedTx.traversal().V(qq1).as("a")
                .V(qq2)
                .addE("link_simple")
                .property(T.id,uuid)
                .property("link_tid", uuid)
                .property("left_tid", qq2.id())
                .property("right_tid", qq1.id())
                .property("dsr", "程序导入")
                .property("name","关系000")
                .to("a").next();
            Object id = next.id();
            System.out.println(id);
            threadedTx.commit();
        }
    }

    @Test
    public void insertEdgeOther1(){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            String tid="tid001";
            GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                .addV("object_qq")
                .property("name", "我是测试qq1",
                    "startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", "程序导入",
                    "geo", Geoshape.point(22.22, 113.1122))
                .property(DefaultKeywordField.TID.getName(),tid)
                .property(DefaultTextField.TITLE.getName(),"我是测试标签1")
                .property("qq_num","111111","dsr","程序导入")
                .property(T.id, tid);
            Vertex qq1 = qqTraversal.next();
            String tid1="tid002";
            Optional<Vertex> vertexOptional = threadedTx.traversal().V("tid002_29_000").tryNext();
            Vertex qq2=null;
            if(vertexOptional.isPresent()){
                qq2=vertexOptional.get();
            }else {
                GraphTraversal<Vertex, Vertex> qqTraversal1 = threadedTx.traversal()
                    .addV("object_qq")
                    .property("name", "我是测试qq2",
                        "startDate", new Date(),
                        "endDate", new Date(),
                        "dsr", "程序导入",
                        "geo", Geoshape.point(22.22, 113.1122))
                    .property(DefaultKeywordField.TID.getName(), tid)
                    .property(DefaultTextField.TITLE.getName(), "我是测试标签2")
                    .property("qq_num", "222222", "dsr", "程序导入")
                    .property(T.id, tid1);
                qq2 = qqTraversal1.next();
            }
            String uuid = "link1";
            RelationIdentifier edgeId = threadedTx.getEdgeId(uuid, "link_simple", (JanusGraphVertex) qq2, (JanusGraphVertex) qq1);
            Optional<Edge> edgeOption = threadedTx.traversal().E("link1_30-tid002_29_000-2112010101-tid001_30_000").tryNext();
            if(edgeOption.isPresent()) {
                Edge edge = edgeOption.get();
                edge.property("grade",123);
                edge.property("time",new Date());
                edge.property("age1",18);
                edge.property("name", "关系001");
                System.out.println("存在！！！！");
                System.out.println(edgeId.equals(edge.id()));
            }else{
                Edge next = threadedTx.traversal().V(qq1).as("a")
                    .V(qq2)
                    .addE("link_simple")
                    .property(T.id, uuid)
                    .property("link_tid", uuid)
                    .property("left_tid", qq2.id())
                    .property("right_tid", qq1.id())
                    .property("dsr", "程序导入")
                    .property("name", "关系000")
                    .to("a").next();
                Object id = next.id();
                System.out.println(id);
                System.out.println(edgeId.equals(id));
            }

            threadedTx.commit();
        }
    }

    @Test
    public void selectEdge(){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            List<? extends Property<Object>> properties = threadedTx.traversal().E("link1_30-tid002_29_000-2112010101-tid001_30_000").properties().toList();
            for(Property<Object> property:properties){
                System.out.println(property.key()+"----------》"+property.value());
            }
            threadedTx.rollback();
        }
    }

    @Test
    public void selectEdge1(){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            List<? extends Property<Object>> properties = threadedTx.traversal().V("tid002_29_000").outE().properties().toList();
            for(Property<Object> property:properties){
                System.out.println(property.key()+"----------》"+property.value());
            }
            threadedTx.rollback();
        }
    }


    @Test
    public void updateProperty(){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            String tid="tid002";
            String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            KydsjTraversalSource kg = threadedTx.traversal(KydsjTraversalSource.class);
            kg.T(tid).properties("name").hasValue("我是测试qq111").drop();
            kg.T(tid).property("name","我是新值").next();
            threadedTx.commit();
        }
    }

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
                .property("name", "我是测试qq111",
                    "startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", "程序导入22222",
                    "geo", Geoshape.point(23.22, 114.1122))
                .property(T.id, tid);
            Vertex qq = qqTraversal.next();
            threadedTx.commit();
        }
    }


    @Test
    public void uDsr(){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                .addV("object_qq")
                .property("name", "SFYmLABST31Ltv8pvVmk7MtyVjhX8C",
                    "startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", "程序导入b-s",
                    "geo", Geoshape.point(22.22, 113.1122))
                .property(T.id, "qq$crAdrvnl4WM");
            Vertex qq = qqTraversal.next();
            threadedTx.commit();
        }
    }

    @Test
    public void appendDsr(){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal().V("qq$crAdrvnl4WM_18_000")
                .property("name", "SFYmLABST31Ltv8pvVmk7MtyVjhX8C",
                    "startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", "程序导入2-1",
                    "geo", Geoshape.point(22.22, 113.1122));
            Vertex qq = qqTraversal.next();
            threadedTx.commit();
        }
    }

    @Test
    public void delete1(){
        try {
            if (g == null) {
                return;
            }
            LOGGER.info("deleting elements");
            List<Vertex> vertices = g.V("tid002_29_000").toList();
            for(Vertex vertex:vertices){
                Object id = vertex.id();
                String label = vertex.label();
                System.out.println("id->"+id);
                System.out.println("label->"+label);
                vertex.remove();
            }
            g.tx().commit();
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
            g.T("tid001").drop().iterate();
            g.tx().commit();
        } catch (Exception e) {
            g.tx().rollback();
        }
    }

    @Test
    public void tt(){
        insertDate("dsr1");
        insertDate("dsr2");
        insertDate("dsr3");
    }
    @Test
    public void dd(){
        insertData("dsr12");
    }

    public void insertData(String dsr){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            String tid="tid001";
            String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String ds="2022-10-10 12:12:12";
            Date date = sdf.parse(ds);
            KydsjTraversalSource kydsj = threadedTx.traversal(KydsjTraversalSource.class);
            Optional<Vertex> vertexOptional = kydsj.T(tid).tryNext();
            if(vertexOptional.isPresent()){
                Vertex vertex = vertexOptional.get();
                CacheVertex cacheVertex=(CacheVertex)vertex;
                Iterator<VertexProperty<Object>> properties = cacheVertex.properties("name");
                while (properties.hasNext()){
                    VertexProperty<Object> next = properties.next();
                    next.remove();
                }
                cacheVertex.trsProperty("name",ds,"dsr",dsr,"startDate", new Date(),
                    "endDate", new Date(),
                    "geo", Geoshape.point(22.22, 113.1122));
                cacheVertex.trsProperty("time",date,"dsr",dsr);
                //vertex.property("db",123,"dsr","dsr2");
            }
            threadedTx.commit();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertDate(String dsr){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            String tid="tid001";
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
                .property(DefaultTextField.TITLE.getName(),"我是测试标签")
                .property(T.id, tid);
            Vertex qq = qqTraversal.next();
            threadedTx.commit();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void deleteDocument(){
        String id="tid001_30_000";
        graph.deleteIndexDocument("object_qq",id);
    }

    @Test
    public void removeNameDsr(){
        String id="tid001_30_000";
        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.buildTransaction().consistencyChecks(false)
            .checkInternalVertexExistence(false).checkExternalVertexExistence(true).start();
        KydsjTraversalSource g = tx.traversal(KydsjTraversalSource.class);
        Stopwatch started = Stopwatch.createStarted();
        boolean change=false;
        Optional<Vertex> vertex = g.V(id).tryNext();
        if(vertex.isPresent()) {
            Vertex next = vertex.get();
            Iterator<VertexProperty<Object>> properties = next.properties("name");
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
                            //if(value1.equals("dsr1")){
                                dsr.remove();
                                change=true;
                            //}
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
        log.info("用时："+started.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    public void removeDsr(){
        String id="tid001_30_000";
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
                            if(value1.equals("dsr1")){
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
        log.info("用时："+started.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    public void deleteObject() {
        // StandardJanusGraphTx threadedTx = janusGraph.tx().createThreadedTx();
        StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) getJanusGraph().buildTransaction()
            .consistencyChecks(false)
            .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start();
        try {
            List<Vertex> vertices = threadedTx.traversal().V().limit(100).toList();
            for(Vertex vertex:vertices){
                //threadedTx.traversal(KydsjTraversalSource.class).V("5a8026b61b1c95978e774448eea80788_8_000").dropExpand().tryNext();
                //threadedTx.traversal(KydsjTraversalSource.class).V("tidxxx000_7_000").dropExpand().tryNext();
                threadedTx.traversal(KydsjTraversalSource.class).V(vertex.id()).dropExpand().tryNext();
             }
            threadedTx.commit();
        } catch (Exception e) {
            if (threadedTx.isOpen()) {
                threadedTx.rollback();
            }
            e.printStackTrace();
        }
    }
    @Test
    public void deleteElements() {
        try {
            if (g == null) {
                return;
            }
            LOGGER.info("deleting elements");
            List<Vertex> vertices = g.V().hasLabel("object_qq").has("qq_num", Text.textContains("4RxhGQLmMLT")).toList();
            for(Vertex vertex:vertices){
                Object id = vertex.id();
                String label = vertex.label();
                vertex.remove();
            }
            g.V("1568_10_000").properties("age1","tid").drop().iterate();
            //g.V().has("name", "pluto").drop().iterate();
            g.tx().commit();
        } catch (Exception e) {
            g.tx().rollback();
        }
    }


    public void createElements(boolean autoId,int thread,int preThreadSize) {
        try {
            LOGGER.info("多线程程序写入大量qq和qq群信息");
            Stopwatch started = Stopwatch.createStarted();
            ExecutorService pool = Executors.newFixedThreadPool(thread,
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("import-data-%d").build());//定义线程数
            List<Future<Integer>> futures= Lists.newArrayList();
            for(int t=0;t<thread;t++) {
                Future<Integer> submit = pool.submit(() -> {
                    int threadTotal = preThreadSize;
                    List<QQData> qqDataList=new ArrayList<>();
                    for (int i = 0; i < threadTotal; i++) {
                        int qqqun_num = new Random().nextInt(1000);
                        String qq_num=RandomStringUtils.randomAlphanumeric(11);
                        QQData data = QQData.builder()
                            .qq_age(new Random().nextInt(100))
                            .qq_num(qq_num)
                            .qq_dengji(new Random().nextInt(100))
                            .qq_date(new Date())
                            .qq_title(RandomStringUtils.randomAlphanumeric(30))
                            .qqqun_date(new Date())
                            .qqqun_num(qqqun_num+"")
                            //.qqqun_num(qq_qunn)
                            //.qqqun_num(RandomStringUtils.randomAlphanumeric(11))
                            //.qqqun_title(String.format("插入的qq群号是%s的QQ群,线程%s",qqqun_num,Thread.currentThread().getName()))
                            .qqqun_title(String.format("插入的qq群号是%s的QQ群",qqqun_num))
                            .text("我是qq群的说明"+qqqun_num)
                            .build();
                        qqDataList.add(data);
                        if(qqDataList.size()==1000){
                            this.runWrite(autoId,qqDataList);
                            qqDataList=new ArrayList<>();
                        }
                    }
                    if(qqDataList.size()>0){
                        this.runWrite(autoId,qqDataList);
                        qqDataList=new ArrayList<>();
                    }
                    LOGGER.info(String.format("当前线程%s,一共处理了->%s条", Thread.currentThread().getName(), threadTotal));
                    return threadTotal;
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

    private void runWrite(boolean autoId,List<QQData> qqDataList) throws java.util.concurrent.ExecutionException, com.github.rholder.retry.RetryException {
        Retryer<Integer> retryer = RetryerBuilder.<Integer>newBuilder()
            .retryIfException()
            .withStopStrategy(StopStrategies.stopAfterAttempt(30))
            .withWaitStrategy(WaitStrategies.fixedWait(300, TimeUnit.MILLISECONDS))
            .build();
        retryer.call(() -> {
            Stopwatch started = Stopwatch.createStarted();
            try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                .consistencyChecks(false)
                .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
                //StandardJanusGraphTx threadedTx = (StandardJanusGraphTx)this.getJanusGraph().tx().createThreadedTx();
                for (QQData qqData : qqDataList) {
                    GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                        .addV("object_qq")
                        .property("tid", qqData.getQq_num())
                        .property("name", qqData.getQq_title(),
                            "startDate", new Date(),
                            "endDate", new Date(),
                            "dsr", String.format("程序导入%s",Thread.currentThread().getName()),
                            "geo", Geoshape.point(22.22, 113.1122))
                        .property("grade", qqData.getQq_dengji(),
                            "startDate", new Date(),
                            "endDate", new Date(),
                            "dsr", "程序导入",
                            "geo", Geoshape.point(22.22, 113.1122))
                        .property("qq_num", qqData.getQq_num(),
                            "startDate", new Date(),
                            "endDate", new Date(),
                            "dsr", "程序导入",
                            "geo", Geoshape.point(22.22, 113.1122))
                        .property("time", qqData.getQq_date(),
                            "startDate", new Date(),
                            "endDate", new Date(),
                            "dsr", "程序导入",
                            "geo", Geoshape.point(22.22, 113.1122))
                        .property("dsr", RandomStringUtils.randomAlphabetic(5))
                        .property("age1", qqData.getQq_age(),
                            "startDate", new Date(),
                            "endDate", new Date(),
                            "dsr", "程序导入",
                            "geo", Geoshape.point(22.22, 113.1122));
                    if(!autoId){
                        qqTraversal.property(T.id, qqData.getQq_num());
                    }
                    Vertex qq = qqTraversal.next();
                    GraphTraversal<Vertex, Vertex> qqqunTraversal = threadedTx.traversal().addV("object_qqqun")
                        .property("tid", qqData.getQqqun_num())
                        .property("name", qqData.getQqqun_title(),
                            "startDate", new Date(),
                            "endDate", new Date(),
                            "dsr", String.format("程序导入%s",Thread.currentThread().getName()),
                            "geo", Geoshape.point(22.22, 113.1122))
                        .property("time", qqData.getQqqun_date(),
                            "startDate", new Date(),
                            "endDate", new Date(),
                            "dsr", "程序导入",
                            "geo", Geoshape.point(22.22, 113.1122))
                        .property("qqqun_num", qqData.getQqqun_num(),
                            "startDate", new Date(),
                            "endDate", new Date(),
                            "dsr", "程序导入",
                            "geo", Geoshape.point(22.22, 113.1122))
                        .property("dsr", RandomStringUtils.randomAlphabetic(5))
                        .property("text", qqData.getText(),
                            "startDate", new Date(),
                            "endDate", new Date(),
                            "dsr", "程序导入",
                            "geo", Geoshape.point(22.22, 113.1122));
                    if(!autoId){
                        qqqunTraversal.property(T.id, qqData.getQqqun_num());
                    }
                    Vertex qqqun = qqqunTraversal.next();;
                    String uuid = UUID.randomUUID().toString();
                    threadedTx.traversal().V(qq.id()).as("a").V(qqqun.id())
                        .addE("link_simple")
                        .property("link_tid", uuid)
                        .property("left_tid", "left_tid1")
                        .property("right_tid", "right_tid1")
                        .property("dsr", RandomStringUtils.randomAlphabetic(4))
                        .to("a").next();
                }
                threadedTx.commit();
                started.stop();
                LOGGER.info(String.format("当前线程%s,已经处理了->%s条用时%s", Thread.currentThread().getName(), qqDataList.size(),started.elapsed(TimeUnit.MILLISECONDS)));
                return qqDataList.size();
            }
        });
    }

    @Data
    @Builder
    static class QQData{
        //qq信息
        private String qq_title;
        private int qq_dengji;
        private int qq_age;
        private Date qq_date;
        private String qq_num;
        //QQ群信息
        private String qqqun_title;
        private String text;
        private Date qqqun_date;
        private String qqqun_num;
    }
}
