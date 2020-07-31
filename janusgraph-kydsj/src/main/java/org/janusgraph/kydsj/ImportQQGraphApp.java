package org.janusgraph.kydsj;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author: ldp
 * @time: 2020/7/20 13:42
 * @jira:
 */
public class ImportQQGraphApp extends JanusGraphApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(ImportQQGraphApp.class);
    public ImportQQGraphApp(String fileName) {
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
        management.makePropertyKey("qq_num").dataType(String.class).make();
        management.makePropertyKey("qqqun_num").dataType(String.class).make();
        management.makePropertyKey("text").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("time").dataType(Date.class).make();
        management.makePropertyKey("age").dataType(Integer.class).make();

        //属性内置属性定义
        management.makePropertyKey("startDate").dataType(Date.class).make();
        management.makePropertyKey("endDate").dataType(Date.class).make();
        management.makePropertyKey("geo").dataType(Geoshape.class).make();
        management.makePropertyKey("dsr").dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("role").dataType(String.class).make();
    }

    /**
     * Creates the composite indexes. A composite index is best used for
     * exact match lookups.
     */
    @Override
    protected void createCompositeIndexes(final JanusGraphManagement management) {
        management.buildIndex("nameIndex", Vertex.class).addKey(management.getPropertyKey("name")).buildCompositeIndex();
    }

    /**
     * Creates the mixed indexes. A mixed index requires that an external
     * indexing backend is configured on the graph instance. A mixed index
     * is best for full text search, numerical range, and geospatial queries.
     */
    @Override
    protected void createMixedIndexes(final JanusGraphManagement management) {
        if (useMixedIndex) {
            management.buildIndex("object_qq", Vertex.class)
                .addKey(management.getPropertyKey("name"))
                .addKey(management.getPropertyKey("grade"))
                .addKey(management.getPropertyKey("qq_num"))
                .addKey(management.getPropertyKey("time"))
                .addKey(management.getPropertyKey("age"))
                .indexOnly(management.getVertexLabel("object_qq"))
                .buildMixedIndex(mixedIndexConfigName);
            management.buildIndex("object_qqqun", Vertex.class)
                .addKey(management.getPropertyKey("name"))
                .addKey(management.getPropertyKey("time"))
                .addKey(management.getPropertyKey("qqqun_num"))
                .addKey(management.getPropertyKey("text"))
                .indexOnly(management.getVertexLabel("object_qqqun"))
                .buildMixedIndex(mixedIndexConfigName);
            /*
            management.buildIndex("eReasonPlace", Edge.class).addKey(management.getPropertyKey("reason"))
                .addKey(management.getPropertyKey("place")).buildMixedIndex(mixedIndexConfigName);*/
        }
    }

    /**
     * Adds the vertices, edges, and properties to the graph.
     */
    public void createElements() {
        try {
            LOGGER.info("多线程程序写入大量qq和qq群信息");
            ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()*2,
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("import-data-%d").build());//定义线程数
            List<Future<Integer>> futures= Lists.newArrayList();
            for(int t=0;t<10;t++) {
                Future<Integer> submit = pool.submit(() -> {
                    //JanusGraphTransaction janusGraphTransaction = this.getJanusGraph().newTransaction();
                    StandardJanusGraphTx threadedTx = (StandardJanusGraphTx)this.getJanusGraph().tx().createThreadedTx();
                    int threadTotal = 1000000;
                    for (int i = 0; i < threadTotal; i++) {
                        QQData qqData = QQData.builder()
                            .qq_age(new Random().nextInt(100))
                            .qq_num(RandomStringUtils.randomAlphanumeric(11))
                            .qq_dengji(new Random().nextInt(100))
                            .qq_date(new Date())
                            .qq_title(RandomStringUtils.randomAlphanumeric(30))
                            .qqqun_date(new Date())
                            .qqqun_num(RandomStringUtils.randomAlphanumeric(11))
                            .qqqun_title(RandomStringUtils.randomAlphanumeric(30))
                            .text(RandomStringUtils.randomAlphanumeric(50))
                            .build();
                        final Vertex qq =threadedTx.traversal().addV("object_qq")
                            .property("name", qqData.getQq_title(),
                                "startDate",new Date(),
                                "endDate",new Date(),
                                "dsr","程序导入",
                                "geo",Geoshape.point(22.22,113.1122))
                            .property("grade", qqData.getQq_dengji(),
                                "startDate",new Date(),
                                "endDate",new Date(),
                                "dsr","程序导入",
                                "geo",Geoshape.point(22.22,113.1122))
                            .property("qq_num", qqData.getQq_num(),
                                "startDate",new Date(),
                                "endDate",new Date(),
                                "dsr","程序导入",
                                "geo",Geoshape.point(22.22,113.1122))
                            .property("time", qqData.getQq_date(),
                                "startDate",new Date(),
                                "endDate",new Date(),
                                "dsr","程序导入",
                                "geo",Geoshape.point(22.22,113.1122))
                            .property("age", qqData.getQq_age(),
                                "startDate",new Date(),
                                "endDate",new Date(),
                                "dsr","程序导入",
                                "geo",Geoshape.point(22.22,113.1122))
                            .next();
                        final Vertex qqqun =threadedTx.traversal().addV("object_qqqun")
                            .property("name", qqData.getQqqun_title(),
                                "startDate",new Date(),
                                "endDate",new Date(),
                                "dsr","程序导入",
                                "geo",Geoshape.point(22.22,113.1122))
                            .property("time", qqData.getQqqun_date(),
                                "startDate",new Date(),
                                "endDate",new Date(),
                                "dsr","程序导入",
                                "geo",Geoshape.point(22.22,113.1122))
                            .property("qqqun_num", qqData.getQqqun_num(),
                                "startDate",new Date(),
                                "endDate",new Date(),
                                "dsr","程序导入",
                                "geo",Geoshape.point(22.22,113.1122))
                            .property("text", qqData.getText(),
                                "startDate",new Date(),
                                "endDate",new Date(),
                                "dsr","程序导入",
                                "geo",Geoshape.point(22.22,113.1122)).next();
                        threadedTx.traversal().V(qq).as("a").V(qqqun).addE("link_simple").to("a").next();
                        if (i % 1000 == 0) {
                            if (supportsTransactions) {
                                threadedTx.commit();
                                threadedTx = (StandardJanusGraphTx)this.getJanusGraph().tx().createThreadedTx();
                                LOGGER.info(String.format("当前线程%s,已经处理了->%s条", Thread.currentThread().getName(), i));
                            }
                        }
                    }
                    if (supportsTransactions) {
                        threadedTx.commit();
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
            LOGGER.info(String.format("所有线程,一共处理了->%s条", total));
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

    @Override
    public void createSchema() {
        final JanusGraphManagement management = getJanusGraph().openManagement();
        try {
            // ;
            if (management.getRelationTypes(RelationType.class).iterator().hasNext()) {
                management.rollback();
                return;
            }
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

    /**
     * Runs some traversal queries to get data from the graph.
     */
    public void readElements() {
        try {
            if (g == null) {
                return;
            }
            LOGGER.info("reading elements");
            // look up vertex by name can use a composite index in JanusGraph
            //final List<Map<Object, Object>> v = g.V().hasLabel("person","teacher").has("name","张三").has("age", P.eq(66)).valueMap(true).next(2);
            Vertex next = g.V().hasLabel("object_qqqun").has(
                "name", Text.textContains("Xr5AF6Fi0LelDjcIDkffOCmVrkGJ3R"))
                .out("link_simple").next();
            // numerical range query can use a mixed index in JanusGraph
            LOGGER.info(next.toString());


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
            if (supportsSchema) {
                createSchema();
            }
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
        final ImportQQGraphApp app = new ImportQQGraphApp(fileName);
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
