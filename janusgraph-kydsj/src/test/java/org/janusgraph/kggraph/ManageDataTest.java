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
import org.apache.commons.lang.RandomStringUtils;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author: ldp
 * @time: 2021/1/18 14:46
 * @jira:
 */
public class ManageDataTest extends AbstractKGgraphTest{
    private static final Logger LOGGER = LoggerFactory.getLogger(ManageDataTest.class);

    @Test
    public void insertAutoIdData(){
        createElements(true,10,10000);
    }

    @Test
    public void insertTidIdData(){
        createElements(false,10,10000);
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
                        QQData data = QQData.builder()
                            .qq_age(new Random().nextInt(100))
                            .qq_num(RandomStringUtils.randomAlphanumeric(11))
                            .qq_dengji(new Random().nextInt(100))
                            .qq_date(new Date())
                            .qq_title(RandomStringUtils.randomAlphanumeric(30))
                            .qqqun_date(new Date())
                            .qqqun_num(qqqun_num+"xbbkg123")
                            //.qqqun_num(qq_qunn)
                            //.qqqun_num(RandomStringUtils.randomAlphanumeric(11))
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
                .consistencyChecks(true)
                .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
                //StandardJanusGraphTx threadedTx = (StandardJanusGraphTx)this.getJanusGraph().tx().createThreadedTx();
                for (QQData qqData : qqDataList) {
                    Vertex qq = threadedTx.traversal()
                        .addV("object_qq")
                        .property(T.id,qqData.getQq_num())
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
                        .property(T.id,qqData.getQqqun_num())
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
