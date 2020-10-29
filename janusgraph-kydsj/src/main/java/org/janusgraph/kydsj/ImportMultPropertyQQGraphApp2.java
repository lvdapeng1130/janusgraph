package org.janusgraph.kydsj;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.util.encoding.LongEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author: ldp
 * @time: 2020/7/20 13:42
 * @jira:
 */
public class ImportMultPropertyQQGraphApp2 extends JanusGraphApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(ImportMultPropertyQQGraphApp2.class);
    public ImportMultPropertyQQGraphApp2(String fileName) {
        super(fileName);
    }

    /**
     * Creates the vertex labels.
     */
    @Override
    protected void createVertexLabels(final JanusGraphManagement management) {
        management.makeVertexLabel("object_qq").make();
        management.makeVertexLabel("object_qqqun").make();
    }

    /**
     * Creates the edge labels.
     */
    @Override
    protected void createEdgeLabels(final JanusGraphManagement management) {
        management.makeEdgeLabel("link_simple").make();
    }

    /**
     * Creates the properties for vertices, edges, and meta-properties.
     */
    @Override
    protected void createProperties(final JanusGraphManagement management) {
        management.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("grade").dataType(Integer.class).make();
        management.makePropertyKey("qq_num").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("qqqun_num").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("text").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("time").dataType(Date.class).make();
        management.makePropertyKey("age1").dataType(Integer.class).make();
        //management.makePropertyKey("linktid").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        //management.makePropertyKey("tid").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("merge_to").dataType(Long.class).cardinality(Cardinality.SINGLE).make();

       /* management.makePropertyKey("startDate").dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("endDate").dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("geo").dataType(Geoshape.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("dsr").dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("role").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("status").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("attachment").dataType(String.class).cardinality(Cardinality.SINGLE).make();*/

      /*  //属性内置属性定义
        management.makePropertyKey("startDate").dataType(Date.class).make();
        management.makePropertyKey("endDate").dataType(Date.class).make();
        management.makePropertyKey("geo").dataType(Geoshape.class).make();
        management.makePropertyKey("dsr").dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("role").dataType(String.class).make();*/
    }

    /**
     * Creates the composite indexes. A composite index is best used for
     * exact match lookups.
     */
    @Override
    protected void createCompositeIndexes(final JanusGraphManagement management) {
        JanusGraphIndex janusGraphIndex = management.buildIndex("qqqun_num_composite_index", Vertex.class)
            .addKey(management.getPropertyKey("qqqun_num")).buildCompositeIndex();
        JanusGraphIndex janusGraphIndex1 = management.buildIndex("qq_num_composite_index", Vertex.class)
            .addKey(management.getPropertyKey("qq_num")).buildCompositeIndex();
        //关系索引
        JanusGraphIndex edgeIndex = management.buildIndex("eget_link_tid", Edge.class)
            .addKey(management.getPropertyKey("linktid")).buildCompositeIndex();
        //management.setConsistency(janusGraphIndex, ConsistencyModifier.LOCK);
        //management.setConsistency(janusGraphIndex1, ConsistencyModifier.LOCK);

    }

    /**
     * Creates the mixed indexes. A mixed index requires that an external
     * indexing backend is configured on the graph instance. A mixed index
     * is best for full text search, numerical range, and geospatial queries.
     */
    @Override
    protected void createMixedIndexes(final JanusGraphManagement management) {
        if (useMixedIndex) {
            //对象的混合索引
            JanusGraphIndex janusGraphIndex = management.buildIndex("object_qq", Vertex.class)
                .addKey(management.getPropertyKey("name"))
                .addKey(management.getPropertyKey("grade"))
                .addKey(management.getPropertyKey("qq_num"))
                .addKey(management.getPropertyKey("time"))
                .addKey(management.getPropertyKey("age1"))
                .indexOnly(management.getVertexLabel("object_qq"))
                .buildMixedIndex(mixedIndexConfigName);
            management.buildIndex("object_qqqun", Vertex.class)
                .addKey(management.getPropertyKey("name"))
                .addKey(management.getPropertyKey("time"))
                .addKey(management.getPropertyKey("qqqun_num"))
                .addKey(management.getPropertyKey("text"))
                .indexOnly(management.getVertexLabel("object_qqqun"))
                .buildMixedIndex(mixedIndexConfigName);

            management.buildIndex("name_index", Vertex.class)
                .addKey(management.getPropertyKey("name"), Mapping.TEXTSTRING.asParameter())
                .buildMixedIndex(mixedIndexConfigName);
            //关系的混合索引
            management.buildIndex("eReasonPlace", Edge.class).addKey(management.getPropertyKey("linktid"))
                .buildMixedIndex(mixedIndexConfigName);
        }
    }

    /**
     * Adds the vertices, edges, and properties to the graph.
     */
    public void createElements() {
        try {
            LOGGER.info("多线程程序写入大量qq和qq群信息");
            Stopwatch started = Stopwatch.createStarted();
            ExecutorService pool = Executors.newFixedThreadPool(10,
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("import-data-%d").build());//定义线程数
            List<Future<Integer>> futures= Lists.newArrayList();
            for(int t=0;t<100;t++) {
                Future<Integer> submit = pool.submit(() -> {
                    //int threadTotal = 1000000;
                    int threadTotal = 10000;
                    List<QQData> qqDataList=new ArrayList<>();
                    for (int i = 0; i < threadTotal; i++) {
                        int qqqun_num = new Random().nextInt(1000);
                        QQData data = QQData.builder()
                            .qq_age(new Random().nextInt(100))
                            .qq_num(RandomStringUtils.randomAlphanumeric(11))
                            .qq_dengji(new Random().nextInt(100))
                            .qq_date(new Date())
                            .qq_title(RandomStringUtils.randomAlphanumeric(30))
                            .qqqun_date(new Date())
                            .qqqun_num(qqqun_num+"")
                            //.qqqun_num(RandomStringUtils.randomAlphanumeric(11))
                            .qqqun_title(String.format("插入的qq群号是%s的QQ群",qqqun_num))
                            .text("我是qq群的说明"+qqqun_num)
                            .build();
                        qqDataList.add(data);
                        if(qqDataList.size()==1000){
                            this.runWrite(qqDataList);
                            qqDataList=new ArrayList<>();
                        }
                    }
                    if (supportsTransactions) {
                        if(qqDataList.size()>0){
                            this.runWrite(qqDataList);
                            qqDataList=new ArrayList<>();
                        }
                        LOGGER.info(String.format("当前线程%s,一共处理了->%s条", Thread.currentThread().getName(), threadTotal));
                    }
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
            if (supportsTransactions) {
                g.tx().rollback();
            }
        }
        if (useMixedIndex) {
            try {
                // mixed indexes typically have a delayed refresh interval
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    private void runWrite(List<QQData> qqDataList) throws java.util.concurrent.ExecutionException, com.github.rholder.retry.RetryException {
        Retryer<Integer> retryer = RetryerBuilder.<Integer>newBuilder()
            .retryIfException()
            .withStopStrategy(StopStrategies.stopAfterAttempt(30))
            .withWaitStrategy(WaitStrategies.fixedWait(300, TimeUnit.MILLISECONDS))
            .build();
        retryer.call(() -> {
            Stopwatch started = Stopwatch.createStarted();
            try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction().consistencyChecks(false)
                .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
                //StandardJanusGraphTx threadedTx = (StandardJanusGraphTx)this.getJanusGraph().tx().createThreadedTx();
                for (QQData qqData : qqDataList) {
                    Vertex qq = threadedTx.traversal().addV("object_qq")
                        .property("tid",qqData.getQq_num())
                        .property("name", qqData.getQq_title(),
                        "startDate", new Date(),
                        "endDate", new Date(),
                        "dsr", "程序导入",
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
                        .property("age1", qqData.getQq_age(),
                            "startDate", new Date(),
                            "endDate", new Date(),
                            "dsr", "程序导入",
                            "geo", Geoshape.point(22.22, 113.1122))
                        .next();
                    Vertex qqqun = threadedTx.traversal().addV("object_qqqun")
                        .property("tid",qqData.getQqqun_num())
                        .property("name", qqData.getQqqun_title(),
                            "startDate", new Date(),
                            "endDate", new Date(),
                            "dsr", "程序导入",
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
                        .property("text", qqData.getText(),
                            "startDate", new Date(),
                            "endDate", new Date(),
                            "dsr", "程序导入",
                            "geo", Geoshape.point(22.22, 113.1122)).next();;
                    String uuid = UUID.randomUUID().toString();
                    threadedTx.traversal().V(qq.id()).as("a").V(qqqun.id()).addE("link_simple").property("linktid", uuid).to("a").next();
                }
                if (supportsTransactions) {
                    threadedTx.commit();
                    started.stop();
                    LOGGER.info(String.format("当前线程%s,已经处理了->%s条用时%s", Thread.currentThread().getName(), qqDataList.size(),started.elapsed(TimeUnit.MILLISECONDS)));
                }
                return qqDataList.size();
            }
        });
    }

    @Override
    public void createSchema() {
        final JanusGraphManagement management = getJanusGraph().openManagement();
        try {
           /* if (management.getRelationTypes(RelationType.class).iterator().hasNext()) {
                management.rollback();
                return;
            }*/
            LOGGER.info("creating schema");
            createProperties(management);
            createVertexLabels(management);
            createEdgeLabels(management);
            createCompositeIndexes(management);
            createMixedIndexes(management);
            management.commit();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
            management.rollback();
        }
    }

    private void printSchema(){
        final JanusGraphManagement management = getJanusGraph().openManagement();
        String printSchemaStr = management.printSchema();
        LOGGER.info(printSchemaStr);
        management.rollback();
    }

    /**
     * Runs some traversal queries to get data from the graph.
     */
    public void readElements() {
        try {
            if (g == null) {
                return;
            }
            LOGGER.info("reading elements");
            List<Vertex> vertices = g.V().hasLabel("object_qqqun").has("qqqun_num", P.eq("1")).toList();
            List<Vertex> vertices1 = g.V().hasLabel("object_qqqun")
                .has("qqqun_num", P.eq("1")).both().toList();
            GraphTraversal<Vertex, Vertex> both = g.V().hasLabel("object_qqqun")
                .has("qqqun_num", P.eq("1")).both().both();
            List<Vertex> next = both.next(1);
            List<Vertex> next1 = both.next(1);

            Vertex crc = g.V(LongEncoding.decode("9k0")).next();
        } finally {
            // the default behavior automatically starts a transaction for
            // any graph interaction, so it is best to finish the transaction
            // even for read-only graph query operations
            if (supportsTransactions) {
                g.tx().rollback();
            }
        }
    }

    /**
     * Run the entire application:
     * 1. Open and initialize the graph
     * 2. Define the schema
     * 3. Build the graph
     * 4. Run traversal queries to get data from the graph
     * 5. Make updates to the graph
     * 6. Close the graph
     */
    public void runApp() {
        try {
            // open and initialize the graph
            openGraph();

            // define the schema before loading data
            //if (supportsSchema) {
                createSchema();
           // }
            printSchema();
            // build the graph structure
            createElements();
            //createElementsMediaDataAndNote();
            //appendOtherDsr();
            // read to see they were made
            //hideVertex();
            //readElements();
            //indexQuery();

            /*for (int i = 0; i < 3; i++) {
                try {
                    Thread.sleep((long) (Math.random() * 500) + 500);
                } catch (InterruptedException e) {
                    LOGGER.error(e.getMessage(), e);
                }
                // update some graph elements with changes
                updateElements();
                // read to see the changes were made
                readElements();
            }

            // delete some graph elements
            deleteElements();
            // read to see the changes were made
            readElements();*/

            // close the graph
            closeGraph();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }


    public static void main(String[] args) throws Exception {
        final String fileName = (args != null && args.length > 0) ? args[0] : null;
        final boolean drop = (args != null && args.length > 1) && "drop".equalsIgnoreCase(args[1]);
        final ImportMultPropertyQQGraphApp2 app = new ImportMultPropertyQQGraphApp2(fileName);
        if (drop) {
            app.openGraph();
            app.dropGraph();
        } else {
            app.runApp();
        }
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
