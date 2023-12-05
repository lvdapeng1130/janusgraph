package org.janusgraph.drawquery;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.qq.DefaultPropertyKey;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
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

    private Date randomDate(String beginDate, String endDate) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date start = format.parse(beginDate);// 构造开始日期
            Date end = format.parse(endDate);// 构造结束日期
            // getTime()表示返回自 1970 年 1 月 1 日 00:00:00 GMT 以来此 Date 对象表示的毫秒数。
            if (start.getTime() >= end.getTime()) {
                return null;
            }
            long date = random(start.getTime(), end.getTime());
            return new Date(date);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private long random(long begin, long end) {
        long rtn = begin + (long) (Math.random() * (end - begin));
        // 如果返回的是开始时间和结束时间，则递归调用本函数查找随机值
        if (rtn == begin || rtn == end) {
            return random(begin, end);
        }
        return rtn;
    }

    private int createObjLabel1(int size){
        try (StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(false)
            .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
            Random random = new Random();
            for(int i=0;i<size;i++) {
                Date randomDate = randomDate("2010-09-20 10:10:10", "2022-08-23 08:30:30");
                Vertex obj1 = threadedTx.traversal()
                    .addV("objLabel1")
                    .property("cs_name", RandomStringUtils.randomNumeric(9),
                        "dsr", String.format("程序导入%s", Thread.currentThread().getName()))
                    .property("cs_name", i,
                        "dsr", String.format("程序导入%s", Thread.currentThread().getName()))
                    .property("cs_number", random.nextInt(100),
                        "dsr", String.format("程序导入%s", Thread.currentThread().getName()))
                    .property("cs_date", randomDate,
                        "dsr", String.format("程序导入%s", Thread.currentThread().getName()))
                    .property(T.id, "A"+i).next();
            }
            threadedTx.commit();
        }
        return size;
    }
    private int createObjLabel_other(int preSize,int cSize,String prev_profix,String c_profix,String prev_objectType,String c_objectType,String linkType){
        int index=0;
        for(int i=0;i<preSize;i++){
            StandardJanusGraph janusGraph=(StandardJanusGraph)this.getJanusGraph();
            String gid = janusGraph.getIDManager().toVertexId(prev_profix+i);
            try (StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                .consistencyChecks(false)
                .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
                Random random = new Random();
                for(int j=0;j<cSize;j++) {
                    Date randomDate = randomDate("2010-09-20 10:10:10", "2022-08-23 08:30:30");
                    Vertex obj = threadedTx.traversal()
                        .addV(c_objectType)
                        .property("cs_name", RandomStringUtils.randomNumeric(9),
                            "dsr", String.format("程序导入%s", Thread.currentThread().getName()))
                        .property("cs_name", index,
                            "dsr", String.format("程序导入%s", Thread.currentThread().getName()))
                        .property("cs_number", random.nextInt(100),
                            "dsr", String.format("程序导入%s", Thread.currentThread().getName()))
                        .property("cs_number", index,
                            "dsr", String.format("程序导入%s", Thread.currentThread().getName()))
                        .property("cs_date", randomDate,
                            "dsr", String.format("程序导入%s", Thread.currentThread().getName()))
                        .property(T.id, c_profix+index).next();
                    threadedTx.traversal().V(gid).as("a").V(obj.id())
                        .addE(linkType)
                        .property(T.id, uuid())
                        .property(DefaultPropertyKey.LEFT_TID.getKey(), gid)
                        .property(DefaultPropertyKey.LEFT_TYPE.getKey(), prev_objectType)
                        .property(DefaultPropertyKey.RIGHT_TID.getKey(), obj.id())
                        .property(DefaultPropertyKey.RIGHT_TYPE.getKey(), obj.label())
                        .property("dsr", RandomStringUtils.randomAlphabetic(4))
                        .from("a").next();
                    index++;
                }
                threadedTx.commit();
                LOGGER.info("写入数据"+cSize);
            }
        }
        return preSize*cSize;
    }

    @Test
    public void test(){
        try (StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(false)
            .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
            Random random = new Random();
            for(int i=0;i<1;i++) {
                Date randomDate = randomDate("2010-09-20 10:10:10", "2022-08-23 08:30:30");
                Vertex obj1 = threadedTx.traversal()
                    .addV("objLabel0")
                    .property("cs_name", RandomStringUtils.randomNumeric(9),
                        "dsr", String.format("程序导入%s", Thread.currentThread().getName()))
                    .property("geo", Geoshape.point(23,113))
                    .property("cs_name", i,
                        "dsr", String.format("程序导入%s", Thread.currentThread().getName()))
                    .property("cs_number", random.nextInt(100),
                        "dsr", String.format("程序导入%s", Thread.currentThread().getName()))
                    .property("cs_date", randomDate,
                        "dsr", String.format("程序导入%s", Thread.currentThread().getName()))
                    .property(T.id, "TEST"+i).next();
            }
            threadedTx.commit();
        }
    }

    @Test
    public void test2() throws ExecutionException, InterruptedException {
        LOGGER.info("写测试1数据");
        int thread=Runtime.getRuntime().availableProcessors();
        int totalSize=50*10000;
        int batchSize=1000;
        int taskSize=totalSize/batchSize;
        Stopwatch started = Stopwatch.createStarted();
        ExecutorService pool = Executors.newFixedThreadPool(thread,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("import-data-%d").build());//定义线程数
        List<Future<Integer>> futures= Lists.newArrayList();
        for(int t=0;t<taskSize;t++) {
            Future<Integer> submit = pool.submit(new MyCall(t,batchSize,this.getJanusGraph()));
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
    public void test3(){
        Stopwatch started1 = Stopwatch.createStarted();
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(false)
            .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
            int index=0;
            Vertex obj1 = threadedTx.traversal()
                .addV("objLabel0")
                .property("cs_name", "objLabel0_" + index,
                    "dsr", "xxx")
                .property("geo", Geoshape.point(23, 113))
                .property("cs_name", index,
                    "dsr", "xxx")
                .property("cs_number", 22,
                    "dsr", "xxx")
                .property(T.id, "TA" + index).next();

            Vertex obj9 = threadedTx.traversal()
                .addV("objLabel9")
                .property("cs_name", "objLabel9_" + index,
                    "dsr", "xxx")
                .property("geo", Geoshape.point(23, 113))
                .property("cs_name", index,
                    "dsr", "xxx")
                .property("cs_number", 33,
                    "dsr", "xxx")
                .property(T.id, "TB" + index).next();
            //hw_e1->[link_jkhw_e1]->qy_e1
            threadedTx.traversal().V(obj1.id()).as("a").V(obj9.id())
                .addE("linkLabel9")
                .property(T.id, "link" + index)
                .property(DefaultPropertyKey.LEFT_TID.getKey(), obj1.id())
                .property(DefaultPropertyKey.LEFT_TYPE.getKey(), obj1.label())
                .property(DefaultPropertyKey.RIGHT_TID.getKey(), obj9.id())
                .property(DefaultPropertyKey.RIGHT_TYPE.getKey(), obj9.label())
                .property("dsr", "xxxx")
                .from("a").next();
            threadedTx.commit();
            started1.stop();
            LOGGER.info(String.format("当前线程%s,条用时%s", Thread.currentThread().getName(),
                started1.elapsed(TimeUnit.MILLISECONDS)));
        }
    }

    @Test
    public void bulidData1() throws ExecutionException, InterruptedException {
        LOGGER.info("写测试1数据");
        Stopwatch started = Stopwatch.createStarted();
        int size1 = this.createObjLabel1(10);
        int size2 = this.createObjLabel_other(size1, 10,"A","B","objLabel1",
            "objLabel2","linkLabel1");
        int size3 = this.createObjLabel_other(size2, 10,"B","C","objLabel2",
            "objLabel3","linkLabel2");
        int size4 = this.createObjLabel_other(size3, 10,"C","D","objLabel3",
            "objLabel4","linkLabel3");
        int size5 = this.createObjLabel_other(size4, 10,"D","E","objLabel4",
            "objLabel5","linkLabel4");
        int size6 = this.createObjLabel_other(size1, 10,"A","F","objLabel1",
            "objLabel6","linkLabel5");
        int size7 = this.createObjLabel_other(size6, 10,"F","H","objLabel6",
            "objLabel7","linkLabel6");
        int size8 = this.createObjLabel_other(size7, 10,"H","G","objLabel7",
            "objLabel8","linkLabel7");

        started.stop();
        LOGGER.info(String.format("用时%s",started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void bulidData2() throws ExecutionException, InterruptedException {
        LOGGER.info("写测试2数据");
        Stopwatch started = Stopwatch.createStarted();
        int size3 = this.createObjLabel_other(1000, 10,"C","M","objLabel3",
            "objLabel9","linkLabel9");
        started.stop();
        LOGGER.info(String.format("用时%s",started.elapsed(TimeUnit.MILLISECONDS)));
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

    static class MyCall implements Callable<Integer> {
        private int p;

        private int batchSize;
        private JanusGraph janusGraph;
        public MyCall(int p,int batchSize,JanusGraph janusGraph){
            this.p=p;
            this.batchSize=batchSize;
            this.janusGraph=janusGraph;
        }
        @Override
        public Integer call() throws Exception {
            Stopwatch started1 = Stopwatch.createStarted();
            try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) janusGraph.buildTransaction()
                .consistencyChecks(false)
                .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
                for (int i=0;i<batchSize;i++) {
                    int index=batchSize*p+i;
                    Vertex obj1 = threadedTx.traversal()
                        .addV("objLabel0")
                        .property("cs_name", "objLabel0_"+index,
                            "dsr", "xxx")
                        .property("geo", Geoshape.point(23,113))
                        .property("cs_name", index,
                            "dsr", "xxx")
                        .property("cs_number",22,
                            "dsr", "xxx")
                        .property(DefaultPropertyKey.UPDATE_DATE.getKey(),new Date())
                        .property(DefaultPropertyKey.TITLE.getKey(),"objLabel0_"+index)
                        .property(T.id, "TA"+index).next();

                    Vertex obj9 = threadedTx.traversal()
                        .addV("objLabel9")
                        .property("cs_name", "objLabel9_"+index,
                            "dsr", "xxx")
                        .property(DefaultPropertyKey.UPDATE_DATE.getKey(),new Date())
                        .property(DefaultPropertyKey.TITLE.getKey(),"objLabel9_"+index)
                        .property("geo", Geoshape.point(23,113))
                        .property("cs_name", index,
                            "dsr", "xxx")
                        .property("cs_number", 33,
                            "dsr", "xxx")
                        .property(T.id, "TB"+index).next();
                    //hw_e1->[link_jkhw_e1]->qy_e1
                    threadedTx.traversal().V(obj1.id()).as("a").V(obj9.id())
                        .addE("linkLabel9")
                        .property(T.id, "link"+index)
                        .property(DefaultPropertyKey.LEFT_TID.getKey(), obj1.id())
                        .property(DefaultPropertyKey.LEFT_TYPE.getKey(), obj1.label())
                        .property(DefaultPropertyKey.RIGHT_TID.getKey(), obj9.id())
                        .property(DefaultPropertyKey.RIGHT_TYPE.getKey(), obj9.label())
                        .property(DefaultPropertyKey.UPDATE_DATE.getKey(),new Date())
                        .property("dsr", "xxxx")
                        .from("a").next();

                }
                threadedTx.commit();
                started1.stop();
                LOGGER.info(String.format("当前线程%s,已经处理了->%s条用时%s", Thread.currentThread().getName(), batchSize,
                    started1.elapsed(TimeUnit.MILLISECONDS)));
                return batchSize;
            }
        }
    }
}
