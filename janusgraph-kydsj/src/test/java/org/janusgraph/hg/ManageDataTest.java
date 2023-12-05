package org.janusgraph.hg;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.qq.DefaultPropertyKey;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 构造海关测试数据
 */
public class ManageDataTest extends AbstractKGgraphTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ManageDataTest.class);
    private String uuid(){
        String s = UUID.randomUUID().toString();
        String id = s.replace("-", "").substring(8, 28);
        return id;
    }

    @Test
    public void bulidData1() throws ExecutionException, InterruptedException {
        LOGGER.info("写测试1数据");
        int thread=Runtime.getRuntime().availableProcessors();
        int totalSize=500*10000;
        int batchSize=1000;
        int taskSize=totalSize/batchSize;
        Stopwatch started = Stopwatch.createStarted();
        ExecutorService pool = Executors.newFixedThreadPool(thread,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("import-data-%d").build());//定义线程数
        List<Future<Integer>> futures= Lists.newArrayList();
        for(int t=0;t<taskSize;t++) {
            Future<Integer> submit = pool.submit(() -> {
                Stopwatch started1 = Stopwatch.createStarted();
                try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                    .consistencyChecks(false)
                    .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
                    for (int i=0;i<batchSize;i++) {
                        Vertex hw_e1 = threadedTx.traversal()
                            .addV("hw_e1")
                            .property("spbhhs", RandomStringUtils.randomNumeric(9),
                                "dsr", String.format("程序导入%s",Thread.currentThread().getName()))
                            .property(T.id, uuid()).next();

                        Vertex qy_e1_1 = threadedTx.traversal().addV("qy_e1")
                            .property(T.id, uuid()).next();;

                        Vertex qy_e1_2 = threadedTx.traversal().addV("qy_e1")
                            .property(T.id, uuid()).next();;

                        String uuid = uuid();
                        //hw_e1->[link_jkhw_e1]->qy_e1
                        threadedTx.traversal().V(hw_e1.id()).as("a").V(qy_e1_1.id())
                            .addE("link_jkhw_e1")
                            .property(T.id, uuid())
                            .property(DefaultPropertyKey.LEFT_TID.getKey(), hw_e1.id())
                            .property(DefaultPropertyKey.LEFT_TYPE.getKey(), hw_e1.label())
                            .property(DefaultPropertyKey.RIGHT_TID.getKey(), qy_e1_1.id())
                            .property(DefaultPropertyKey.RIGHT_TYPE.getKey(), qy_e1_1.label())
                            .property("dsr", RandomStringUtils.randomAlphabetic(4))
                            .from("a").next();
                        threadedTx.traversal().V(hw_e1.id()).as("a").V(qy_e1_2.id())
                            .addE("link_jkhw_e1")
                            .property(T.id, uuid())
                            .property(DefaultPropertyKey.LEFT_TID.getKey(), hw_e1.id())
                            .property(DefaultPropertyKey.LEFT_TYPE.getKey(), hw_e1.label())
                            .property(DefaultPropertyKey.RIGHT_TID.getKey(), qy_e1_2.id())
                            .property(DefaultPropertyKey.RIGHT_TYPE.getKey(), qy_e1_2.label())
                            .property("dsr", RandomStringUtils.randomAlphabetic(4))
                            .from("a").next();

                    }
                    threadedTx.commit();
                    started1.stop();
                    LOGGER.info(String.format("当前线程%s,已经处理了->%s条用时%s", Thread.currentThread().getName(), batchSize,
                        started1.elapsed(TimeUnit.MILLISECONDS)));
                    return batchSize;
                }
            });
            futures.add(submit);
        }
        long total=0;
        for(Future<Integer> future:futures){
            total+=future.get();
        }
        started.stop();
        LOGGER.info(String.format("所有线程,一共处理了->%s条,用时%s", total,started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void bulidData2() throws ExecutionException, InterruptedException {
        LOGGER.info("写测试2数据");
        int thread=Runtime.getRuntime().availableProcessors();
        int totalSize=500*10000;
        int batchSize=1000;
        int taskSize=totalSize/batchSize;
        Stopwatch started = Stopwatch.createStarted();
        ExecutorService pool = Executors.newFixedThreadPool(thread,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("import-data-%d").build());//定义线程数
        List<Future<Integer>> futures= Lists.newArrayList();
        for(int t=0;t<taskSize;t++) {
            Future<Integer> submit = pool.submit(() -> {
                Stopwatch started1 = Stopwatch.createStarted();
                try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                    .consistencyChecks(false)
                    .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
                    for (int i=0;i<batchSize;i++) {
                        Vertex gr_e2 = threadedTx.traversal()
                            .addV("gr_e2")
                            .property(T.id, uuid()).next();

                        Vertex hb_e2 = threadedTx.traversal().addV("hb_e2")
                            .property(T.id, uuid()).next();;

                        Vertex ka_e2 = threadedTx.traversal().addV("ka_e2")
                            .property(T.id, uuid()).next();;

                        String uuid = uuid();
                        //gr_e2->[link_cz_e2]->hb_e2->[link_mdjc_e2]->ka_e2
                        threadedTx.traversal().V(gr_e2.id()).as("a").V(hb_e2.id())
                            .addE("link_cz_e2")
                            .property(T.id, uuid())
                            .property(DefaultPropertyKey.LEFT_TID.getKey(), gr_e2.id())
                            .property(DefaultPropertyKey.LEFT_TYPE.getKey(), gr_e2.label())
                            .property(DefaultPropertyKey.RIGHT_TID.getKey(), hb_e2.id())
                            .property(DefaultPropertyKey.RIGHT_TYPE.getKey(), hb_e2.label())
                            .property("dsr", RandomStringUtils.randomAlphabetic(4))
                            .from("a").next();
                        threadedTx.traversal().V(hb_e2.id()).as("a").V(ka_e2.id())
                            .addE("link_mdjc_e2")
                            .property(T.id, uuid())
                            .property(DefaultPropertyKey.LEFT_TID.getKey(), hb_e2.id())
                            .property(DefaultPropertyKey.LEFT_TYPE.getKey(), hb_e2.label())
                            .property(DefaultPropertyKey.RIGHT_TID.getKey(), ka_e2.id())
                            .property(DefaultPropertyKey.RIGHT_TYPE.getKey(), ka_e2.label())
                            .property("dsr", RandomStringUtils.randomAlphabetic(4))
                            .from("a").next();

                    }
                    threadedTx.commit();
                    started1.stop();
                    LOGGER.info(String.format("当前线程%s,已经处理了->%s条用时%s", Thread.currentThread().getName(), batchSize,
                        started1.elapsed(TimeUnit.MILLISECONDS)));
                    return batchSize;
                }
            });
            futures.add(submit);
        }
        long total=0;
        for(Future<Integer> future:futures){
            total+=future.get();
        }
        started.stop();
        LOGGER.info(String.format("所有线程,一共处理了->%s条,用时%s", total,started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void bulidData3() throws ExecutionException, InterruptedException {
        LOGGER.info("写测试3数据");
        int thread=Runtime.getRuntime().availableProcessors();
        int totalSize=500*10000;
        int batchSize=1000;
        int taskSize=totalSize/batchSize;
        Stopwatch started = Stopwatch.createStarted();
        ExecutorService pool = Executors.newFixedThreadPool(thread,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("import-data-%d").build());//定义线程数
        List<Future<Integer>> futures= Lists.newArrayList();
        for(int t=0;t<taskSize;t++) {
            Future<Integer> submit = pool.submit(() -> {
                Stopwatch started1 = Stopwatch.createStarted();
                try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                    .consistencyChecks(false)
                    .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
                    for (int i=0;i<batchSize;i++) {
                        Vertex qy_e3 = threadedTx.traversal()
                            .addV("qy_e3")
                            .property(T.id, uuid()).next();

                        Vertex v1_e3 = threadedTx.traversal().addV("v1_e3")
                            .property(T.id, uuid()).next();

                        Vertex v2_e3 = threadedTx.traversal().addV("v2_e3")
                            .property(T.id, uuid()).next();
                        Vertex qy_e3_2 = threadedTx.traversal().addV("qy_e3")
                            .property(T.id, uuid()).next();

                        String uuid = uuid();

                        threadedTx.traversal().V(qy_e3.id()).as("a").V(v1_e3.id())
                            .addE("link1_e3")
                            .property(T.id, uuid())
                            .property(DefaultPropertyKey.LEFT_TID.getKey(), qy_e3.id())
                            .property(DefaultPropertyKey.LEFT_TYPE.getKey(), qy_e3.label())
                            .property(DefaultPropertyKey.RIGHT_TID.getKey(), v1_e3.id())
                            .property(DefaultPropertyKey.RIGHT_TYPE.getKey(), v1_e3.label())
                            .property("dsr", RandomStringUtils.randomAlphabetic(4))
                            .from("a").next();

                        threadedTx.traversal().V(v1_e3.id()).as("a").V(v2_e3.id())
                            .addE("link2_e3")
                            .property(T.id, uuid())
                            .property(DefaultPropertyKey.LEFT_TID.getKey(), v1_e3.id())
                            .property(DefaultPropertyKey.LEFT_TYPE.getKey(), v1_e3.label())
                            .property(DefaultPropertyKey.RIGHT_TID.getKey(), v2_e3.id())
                            .property(DefaultPropertyKey.RIGHT_TYPE.getKey(), v2_e3.label())
                            .property("dsr", RandomStringUtils.randomAlphabetic(4))
                            .from("a").next();

                        threadedTx.traversal().V(v2_e3.id()).as("a").V(qy_e3_2.id())
                            .addE("link3_e3")
                            .property(T.id, uuid())
                            .property(DefaultPropertyKey.LEFT_TID.getKey(), v2_e3.id())
                            .property(DefaultPropertyKey.LEFT_TYPE.getKey(), v2_e3.label())
                            .property(DefaultPropertyKey.RIGHT_TID.getKey(), qy_e3_2.id())
                            .property(DefaultPropertyKey.RIGHT_TYPE.getKey(), qy_e3_2.label())
                            .property("dsr", RandomStringUtils.randomAlphabetic(4))
                            .from("a").next();

                    }
                    threadedTx.commit();
                    started1.stop();
                    LOGGER.info(String.format("当前线程%s,已经处理了->%s条用时%s", Thread.currentThread().getName(), batchSize,
                        started1.elapsed(TimeUnit.MILLISECONDS)));
                    return batchSize;
                }
            });
            futures.add(submit);
        }
        long total=0;
        for(Future<Integer> future:futures){
            total+=future.get();
        }
        started.stop();
        LOGGER.info(String.format("所有线程,一共处理了->%s条,用时%s", total,started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void bulidData4() throws ExecutionException, InterruptedException {
        LOGGER.info("写测试4数据");
        int thread=Runtime.getRuntime().availableProcessors();
        int totalSize=500*10000;
        int batchSize=1000;
        int taskSize=totalSize/batchSize;
        Stopwatch started = Stopwatch.createStarted();
        ExecutorService pool = Executors.newFixedThreadPool(thread,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("import-data-%d").build());//定义线程数
        List<Future<Integer>> futures= Lists.newArrayList();
        for(int t=0;t<taskSize;t++) {
            Future<Integer> submit = pool.submit(() -> {
                Stopwatch started1 = Stopwatch.createStarted();
                try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                    .consistencyChecks(false)
                    .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
                    for (int i=0;i<batchSize;i++) {
                        Vertex qy_e4 = threadedTx.traversal()
                            .addV("qy_e4")
                            .property(T.id, uuid()).next();

                        Vertex hw_e4 = threadedTx.traversal().addV("hw_e4")
                            .property("bgdbh_e4", RandomStringUtils.randomNumeric(9),
                                "dsr", String.format("程序导入%s",Thread.currentThread().getName()))
                            .property(DefaultPropertyKey.UPDATE_DATE.getKey(),new Date())
                            .property(T.id, uuid()).next();

                        Vertex zscqbaxx_e4 = threadedTx.traversal().addV("zscqbaxx_e4")
                            .property(T.id, uuid()).next();
                        Vertex qy_e4_2 = threadedTx.traversal().addV("qy_e4")
                            .property(T.id, uuid()).next();

                        threadedTx.traversal().V(hw_e4.id()).as("a").V(qy_e4.id())
                            .addE("link_scxsdw_e4")
                            .property(T.id, uuid())
                            .property(DefaultPropertyKey.LEFT_TID.getKey(), hw_e4.id())
                            .property(DefaultPropertyKey.LEFT_TYPE.getKey(), hw_e4.label())
                            .property(DefaultPropertyKey.RIGHT_TID.getKey(), qy_e4.id())
                            .property(DefaultPropertyKey.RIGHT_TYPE.getKey(), qy_e4.label())
                            .property("dsr", RandomStringUtils.randomAlphabetic(4))
                            .from("a").next();

                        threadedTx.traversal().V(hw_e4.id()).as("a").V(zscqbaxx_e4.id())
                            .addE("link_zscqqq_e4")
                            .property(T.id, uuid())
                            .property(DefaultPropertyKey.LEFT_TID.getKey(), hw_e4.id())
                            .property(DefaultPropertyKey.LEFT_TYPE.getKey(), hw_e4.label())
                            .property(DefaultPropertyKey.RIGHT_TID.getKey(), zscqbaxx_e4.id())
                            .property(DefaultPropertyKey.RIGHT_TYPE.getKey(), zscqbaxx_e4.label())
                            .property("dsr", RandomStringUtils.randomAlphabetic(4))
                            .from("a").next();

                        threadedTx.traversal().V(zscqbaxx_e4.id()).as("a").V(qy_e4_2.id())
                            .addE("link_qlr_e4")
                            .property(T.id, uuid())
                            .property(DefaultPropertyKey.LEFT_TID.getKey(), zscqbaxx_e4.id())
                            .property(DefaultPropertyKey.LEFT_TYPE.getKey(), zscqbaxx_e4.label())
                            .property(DefaultPropertyKey.RIGHT_TID.getKey(), qy_e4_2.id())
                            .property(DefaultPropertyKey.RIGHT_TYPE.getKey(), qy_e4_2.label())
                            .property("dsr", RandomStringUtils.randomAlphabetic(4))
                            .from("a").next();

                    }
                    threadedTx.commit();
                    started1.stop();
                    LOGGER.info(String.format("当前线程%s,已经处理了->%s条用时%s", Thread.currentThread().getName(), batchSize,
                        started1.elapsed(TimeUnit.MILLISECONDS)));
                    return batchSize;
                }
            });
            futures.add(submit);
        }
        long total=0;
        for(Future<Integer> future:futures){
            total+=future.get();
        }
        started.stop();
        LOGGER.info(String.format("所有线程,一共处理了->%s条,用时%s", total,started.elapsed(TimeUnit.MILLISECONDS)));
    }


    @Test
    public void bulidData5() throws ExecutionException, InterruptedException {
        LOGGER.info("写测试5数据");
        int thread=Runtime.getRuntime().availableProcessors();
        int totalSize=500*10000;
        int batchSize=1000;
        int taskSize=totalSize/batchSize;
        Stopwatch started = Stopwatch.createStarted();
        ExecutorService pool = Executors.newFixedThreadPool(thread,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("import-data-%d").build());//定义线程数
        List<Future<Integer>> futures= Lists.newArrayList();
        for(int t=0;t<taskSize;t++) {
            Future<Integer> submit = pool.submit(() -> {
                Stopwatch started1 = Stopwatch.createStarted();
                try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                    .consistencyChecks(false)
                    .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
                    for (int i=0;i<batchSize;i++) {
                        Vertex hw_e5 = threadedTx.traversal()
                            .addV("hw_e5")
                            .property("spbhhs_e5", RandomStringUtils.randomNumeric(9),
                                "dsr", String.format("程序导入%s",Thread.currentThread().getName()))
                            .property("bgdbh_e5", RandomStringUtils.randomNumeric(9),
                                "dsr", String.format("程序导入%s",Thread.currentThread().getName()))
                            .property(T.id, uuid()).next();

                        Vertex bgd_e5 = threadedTx.traversal().addV("bgd_e5")
                            .property(T.id, uuid()).next();

                        Vertex ka_e5 = threadedTx.traversal().addV("ka_e5")
                            .property(T.id, uuid()).next();

                        threadedTx.traversal().V(bgd_e5.id()).as("a").V(hw_e5.id())
                            .addE("link_sbhw_e5")
                            .property(T.id, uuid())
                            .property(DefaultPropertyKey.LEFT_TID.getKey(), bgd_e5.id())
                            .property(DefaultPropertyKey.LEFT_TYPE.getKey(), bgd_e5.label())
                            .property(DefaultPropertyKey.RIGHT_TID.getKey(), hw_e5.id())
                            .property(DefaultPropertyKey.RIGHT_TYPE.getKey(), hw_e5.label())
                            .property("dsr", RandomStringUtils.randomAlphabetic(4))
                            .from("a").next();

                        threadedTx.traversal().V(bgd_e5.id()).as("a").V(ka_e5.id())
                            .addE("link_zyg_e5")
                            .property(T.id, uuid())
                            .property(DefaultPropertyKey.LEFT_TID.getKey(), bgd_e5.id())
                            .property(DefaultPropertyKey.LEFT_TYPE.getKey(), bgd_e5.label())
                            .property(DefaultPropertyKey.RIGHT_TID.getKey(), ka_e5.id())
                            .property(DefaultPropertyKey.RIGHT_TYPE.getKey(), ka_e5.label())
                            .property("dsr", RandomStringUtils.randomAlphabetic(4))
                            .from("a").next();
                    }
                    threadedTx.commit();
                    started1.stop();
                    LOGGER.info(String.format("当前线程%s,已经处理了->%s条用时%s", Thread.currentThread().getName(), batchSize,
                        started1.elapsed(TimeUnit.MILLISECONDS)));
                    return batchSize;
                }
            });
            futures.add(submit);
        }
        long total=0;
        for(Future<Integer> future:futures){
            total+=future.get();
        }
        started.stop();
        LOGGER.info(String.format("所有线程,一共处理了->%s条,用时%s", total,started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void bulidData5_other() throws ExecutionException, InterruptedException {
        LOGGER.info("写测试5数据");
        int thread=Runtime.getRuntime().availableProcessors();
        String tid="123456789";
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(false)
            .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
            Vertex hw_e5 = threadedTx.traversal()
                .addV("hw_e5")
                .property("spbhhs_e5", "111111111111",
                    "dsr", String.format("程序导入%s",Thread.currentThread().getName()))
                .property("bgdbh_e5", "2222222222222",
                    "dsr", String.format("程序导入%s",Thread.currentThread().getName()))
                .property(T.id, tid).next();
            threadedTx.commit();
        }
        StandardJanusGraph janusGraph=(StandardJanusGraph)this.getJanusGraph();
        String gid = janusGraph.getIDManager().toVertexId(tid);
        Stopwatch started = Stopwatch.createStarted();
        ExecutorService pool = Executors.newFixedThreadPool(thread,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("import-data-%d").build());//定义线程数
        List<Future<Integer>> futures= Lists.newArrayList();
        int totalSize=100*10000;
        int batchSize=1000;
        int taskSize=totalSize/batchSize;
        //通过关系类型为link_scqy_e5与100万个qy_e5对象关联；
        for(int t=0;t<taskSize;t++) {
            Future<Integer> submit = pool.submit(() -> {
                Stopwatch started1 = Stopwatch.createStarted();
                try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                    .consistencyChecks(false)
                    .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
                    for (int i=0;i<batchSize;i++) {
                        Vertex qy_e5 = threadedTx.traversal().addV("qy_e5")
                            .property(T.id, uuid()).next();
                        threadedTx.traversal().V(gid).as("a").V(qy_e5.id())
                            .addE("link_scqy_e5")
                            .property(T.id, uuid())
                            .property(DefaultPropertyKey.LEFT_TID.getKey(), gid)
                            .property(DefaultPropertyKey.LEFT_TYPE.getKey(), "hw_e5")
                            .property(DefaultPropertyKey.RIGHT_TID.getKey(), qy_e5.id())
                            .property(DefaultPropertyKey.RIGHT_TYPE.getKey(), qy_e5.label())
                            .property("dsr", RandomStringUtils.randomAlphabetic(4))
                            .from("a").next();
                    }
                    threadedTx.commit();
                    started1.stop();
                    LOGGER.info(String.format("当前线程%s,已经处理了->%s条用时%s", Thread.currentThread().getName(), batchSize,
                        started1.elapsed(TimeUnit.MILLISECONDS)));
                    return batchSize;
                }
            });
            futures.add(submit);
        }
        //通过关系类型为link_sbhw_e5与1万个bgd_e5对象关联；
        for(int i=0;i<10000;i++){
            Object bgd_e5_gid;
            try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                .consistencyChecks(false)
                .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
                Vertex bgd_e5 = threadedTx.traversal().addV("bgd_e5")
                    .property(T.id, uuid()).next();
                bgd_e5_gid=bgd_e5.id();
                threadedTx.traversal().V(gid).as("a").V(bgd_e5.id())
                    .addE("link_sbhw_e5")
                    .property(T.id, uuid())
                    .property(DefaultPropertyKey.LEFT_TID.getKey(), gid)
                    .property(DefaultPropertyKey.LEFT_TYPE.getKey(), "hw_e5")
                    .property(DefaultPropertyKey.RIGHT_TID.getKey(), bgd_e5.id())
                    .property(DefaultPropertyKey.RIGHT_TYPE.getKey(), bgd_e5.label())
                    .property("dsr", RandomStringUtils.randomAlphabetic(4))
                    .from("a").next();
                threadedTx.commit();
            }
            if(bgd_e5_gid!=null){
                //bgd_e5对象，每一个与1万个ka_e5对象关联（通过关系类型link_zyg_e5）
                for(int t=0;t<(10000/batchSize);t++) {
                    Future<Integer> submit = pool.submit(() -> {
                        Stopwatch started1 = Stopwatch.createStarted();
                        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                            .consistencyChecks(false)
                            .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
                            for (int j=0;j<batchSize;j++) {
                                Vertex ka_e5 = threadedTx.traversal().addV("ka_e5")
                                    .property(T.id, uuid()).next();
                                threadedTx.traversal().V(bgd_e5_gid).as("a").V(ka_e5.id())
                                    .addE("link_zyg_e5")
                                    .property(T.id, uuid())
                                    .property(DefaultPropertyKey.LEFT_TID.getKey(), bgd_e5_gid)
                                    .property(DefaultPropertyKey.LEFT_TYPE.getKey(), "bgd_e5")
                                    .property(DefaultPropertyKey.RIGHT_TID.getKey(), ka_e5.id())
                                    .property(DefaultPropertyKey.RIGHT_TYPE.getKey(), ka_e5.label())
                                    .property("dsr", RandomStringUtils.randomAlphabetic(4))
                                    .from("a").next();
                            }
                            threadedTx.commit();
                            started1.stop();
                            LOGGER.info(String.format("当前线程%s,已经处理了->%s条用时%s", Thread.currentThread().getName(), batchSize,
                                started1.elapsed(TimeUnit.MILLISECONDS)));
                            return batchSize;
                        }
                    });
                    futures.add(submit);
                }
                //bgd_e5对象，每一个与1千个qy_e5对象关联（通过关系类型link_t1_e5）
                Future<Integer> qy_e5_submit = pool.submit(() -> {
                    Stopwatch started1 = Stopwatch.createStarted();
                    try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                        .consistencyChecks(false)
                        .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
                        for (int j=0;j<batchSize;j++) {
                            Vertex qy_e5 = threadedTx.traversal().addV("qy_e5")
                                .property(T.id, uuid()).next();
                            threadedTx.traversal().V(bgd_e5_gid).as("a").V(qy_e5.id())
                                .addE("link_t1_e5")
                                .property(T.id, uuid())
                                .property(DefaultPropertyKey.LEFT_TID.getKey(), bgd_e5_gid)
                                .property(DefaultPropertyKey.LEFT_TYPE.getKey(), "bgd_e5")
                                .property(DefaultPropertyKey.RIGHT_TID.getKey(), qy_e5.id())
                                .property(DefaultPropertyKey.RIGHT_TYPE.getKey(), qy_e5.label())
                                .property("dsr", RandomStringUtils.randomAlphabetic(4))
                                .from("a").next();
                        }
                        threadedTx.commit();
                        started1.stop();
                        LOGGER.info(String.format("当前线程%s,已经处理了->%s条用时%s", Thread.currentThread().getName(), batchSize,
                            started1.elapsed(TimeUnit.MILLISECONDS)));
                        return batchSize;
                    }
                });
                futures.add(qy_e5_submit);
                //bgd_e5对象，每一个与1千个gj_e5对象关联（通过关系类型link_t2_e5）
                Future<Integer> gj_e5_submit = pool.submit(() -> {
                    Stopwatch started1 = Stopwatch.createStarted();
                    try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                        .consistencyChecks(false)
                        .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
                        for (int j=0;j<batchSize;j++) {
                            Vertex gj_e5 = threadedTx.traversal().addV("gj_e5")
                                .property(T.id, uuid()).next();
                            threadedTx.traversal().V(bgd_e5_gid).as("a").V(gj_e5.id())
                                .addE("link_t2_e5")
                                .property(T.id, uuid())
                                .property(DefaultPropertyKey.LEFT_TID.getKey(), bgd_e5_gid)
                                .property(DefaultPropertyKey.LEFT_TYPE.getKey(), "bgd_e5")
                                .property(DefaultPropertyKey.RIGHT_TID.getKey(), gj_e5.id())
                                .property(DefaultPropertyKey.RIGHT_TYPE.getKey(), gj_e5.label())
                                .property("dsr", RandomStringUtils.randomAlphabetic(4))
                                .from("a").next();
                        }
                        threadedTx.commit();
                        started1.stop();
                        LOGGER.info(String.format("当前线程%s,已经处理了->%s条用时%s", Thread.currentThread().getName(), batchSize,
                            started1.elapsed(TimeUnit.MILLISECONDS)));
                        return batchSize;
                    }
                });
                futures.add(gj_e5_submit);
            }else {
                throw new RuntimeException("bgd_e5_gid is null");
            }
        }

        long total=0;
        for(Future<Integer> future:futures){
            total+=future.get();
        }
        started.stop();
        LOGGER.info(String.format("所有线程,一共处理了->%s条,用时%s", total,started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void bulidData6() throws ExecutionException, InterruptedException {
        LOGGER.info("写测试6数据");
        int thread=Runtime.getRuntime().availableProcessors();
        int totalSize=1000*10000;
        int batchSize=1000;
        int taskSize=totalSize/batchSize;
        Stopwatch started = Stopwatch.createStarted();
        ExecutorService pool = Executors.newFixedThreadPool(thread,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("import-data-%d").build());//定义线程数
        List<Future<Integer>> futures= Lists.newArrayList();
        for(int t=0;t<taskSize;t++) {
            Future<Integer> submit = pool.submit(() -> {
                Stopwatch started1 = Stopwatch.createStarted();
                try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                    .consistencyChecks(false)
                    .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
                    Random random=new Random();
                    for (int i=0;i<batchSize;i++) {
                        Vertex hb_e6 = threadedTx.traversal()
                            .addV("hb_e6")
                            .property("jcj_e6", RandomStringUtils.randomAlphabetic(random.nextInt(50)),
                                "dsr", String.format("程序导入%s",Thread.currentThread().getName()))
                            .property(T.id, uuid()).next();
                    }
                    threadedTx.commit();
                    started1.stop();
                    LOGGER.info(String.format("当前线程%s,已经处理了->%s条用时%s", Thread.currentThread().getName(), batchSize,
                        started1.elapsed(TimeUnit.MILLISECONDS)));
                    return batchSize;
                }
            });
            futures.add(submit);
        }
        long total=0;
        for(Future<Integer> future:futures){
            total+=future.get();
        }
        started.stop();
        LOGGER.info(String.format("所有线程,一共处理了->%s条,用时%s", total,started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void bulidData7() throws ExecutionException, InterruptedException {
        LOGGER.info("写测试7数据");
        int thread=Runtime.getRuntime().availableProcessors();
        int totalSize=1000*10000;
        int batchSize=1000;
        int taskSize=totalSize/batchSize;
        Stopwatch started = Stopwatch.createStarted();
        ExecutorService pool = Executors.newFixedThreadPool(thread,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("import-data-%d").build());//定义线程数
        List<Future<Integer>> futures= Lists.newArrayList();
        for(int t=0;t<taskSize;t++) {
            Future<Integer> submit = pool.submit(() -> {
                Stopwatch started1 = Stopwatch.createStarted();
                try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                    .consistencyChecks(false)
                    .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
                    for (int i=0;i<batchSize;i++) {
                        Vertex hb_e6 = threadedTx.traversal()
                            .addV("hb_e7")
                            .property("hbrq_e7", date(),
                                "dsr", String.format("程序导入%s",Thread.currentThread().getName()))
                            .property(T.id, uuid()).next();
                    }
                    threadedTx.commit();
                    started1.stop();
                    LOGGER.info(String.format("当前线程%s,已经处理了->%s条用时%s", Thread.currentThread().getName(), batchSize,
                        started1.elapsed(TimeUnit.MILLISECONDS)));
                    return batchSize;
                }
            });
            futures.add(submit);
        }
        long total=0;
        for(Future<Integer> future:futures){
            total+=future.get();
        }
        started.stop();
        LOGGER.info(String.format("所有线程,一共处理了->%s条,用时%s", total,started.elapsed(TimeUnit.MILLISECONDS)));
    }

    private Date date(){
        Random random = new Random();
        int minDay = (int) LocalDate.of(1900, 1, 1).toEpochDay();
        int maxDay = (int) LocalDate.of(2021, 7, 1).toEpochDay();
        long randomDay = minDay + random.nextInt(maxDay - minDay);
        LocalDate randomBirthDate = LocalDate.ofEpochDay(randomDay);
        Instant instant = randomBirthDate.atTime(LocalTime.MIDNIGHT).atZone(ZoneId.systemDefault()).toInstant();
        Date date = Date.from(instant);
        return date;
    }
    @Test
    public void TestDate(){
        Date date = date();
        System.out.println(DateFormatUtils.format(date,"yyyy-MM-dd HH:mm:ss"));
    }
}
