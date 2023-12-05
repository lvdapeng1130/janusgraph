package org.janusgraph.drawquery;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.drawquery.pojo.KgDiagram;
import org.janusgraph.drawquery.pojo.KgPathGraph;
import org.janusgraph.dsl.KydsjTraversal;
import org.janusgraph.qq.DefaultPropertyKey;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janusgraph.dsl.__;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.janusgraph.dsl.__.bothE;

/**
 * 测试海关语句
 */
public class SelectedKGgraph extends AbstractKGgraphTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SelectedKGgraph.class);
    @Test
    public void select1(){
        Stopwatch started = Stopwatch.createStarted();
        List<Map<String, Object>> maps = g.V().as("n_0").hasLabel("hw_e1")
            .has("spbhhs", P.eq("016324466")).bothE("link_jkhw_e1")
            .dedup().by(__.path()).otherV().hasLabel("qy_e1").path()
            .from("n_0").as("p").project("p")
            .by(__.identity()).limit(20).project("p").toList();
        started.stop();
        LOGGER.info(String.format("结果条数%s,用时%s",maps.size(),started.elapsed(TimeUnit.MILLISECONDS)));
    }
    @Test
    public void select2(){
        Stopwatch started = Stopwatch.createStarted();
        /*List<Path> paths = g.V().hasLabel("objLabel1").hasId("A6_21_000").as("a")
            .union(__.V("A0_15_000").hasLabel("objLabel1")).path().toList();*/
       /* List<Path> paths = g.V("A6_21_000").hasLabel("objLabel1").bothE("linkLabel1")
            .otherV().hasLabel("objLabel2").bothE("linkLabel2")
            .otherV().hasLabel("objLabel3").bothE("linkLabel3")
            .otherV().hasLabel("objLabel4").bothE("linkLabel4")
            .otherV().hasLabel("objLabel5").path().limit(5).toList();*/
        List<Path> paths = g.V("A0_15_000").hasLabel("objLabel1").bothE("linkLabel1")
            .otherV().hasLabel("objLabel2").bothE("linkLabel2")
            .otherV().hasLabel("objLabel3").bothE("linkLabel3")
            .otherV().hasLabel("objLabel4").bothE("linkLabel4")
            .otherV().hasLabel("objLabel5").path().limit(5).toList();
        started.stop();
        for(Path path : paths){
            System.out.println(path);
        }
        LOGGER.info(String.format("结果条数%s,用时%s",paths.size(),started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void select3(){
        Stopwatch started = Stopwatch.createStarted();
        List<Path> paths = g.V("E156106_22_000").hasLabel("objLabel5").bothE("linkLabel4")
            .otherV().hasLabel("objLabel4").bothE("linkLabel3")
            .otherV().hasLabel("objLabel3").bothE("linkLabel2")
            .otherV().hasLabel("objLabel2").bothE("linkLabel1")
            .otherV().hasLabel("objLabel1").path().limit(5).toList();
        started.stop();
        for(Path path : paths){
            System.out.println(path);
        }
        LOGGER.info(String.format("结果条数%s,用时%s",paths.size(),started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void select4(){
        Stopwatch started = Stopwatch.createStarted();
        List<Path> paths = g.V("A0_15_000").hasLabel("objLabel1").union(bothE("linkLabel1")
            .otherV().hasLabel("objLabel2").bothE("linkLabel2")
            .otherV().hasLabel("objLabel3").bothE("linkLabel3")
            .otherV().hasLabel("objLabel4").bothE("linkLabel4")
            .otherV().hasLabel("objLabel5").limit(5).path(), bothE("linkLabel5")
            .otherV().hasLabel("objLabel6").bothE("linkLabel6")
            .otherV().hasLabel("objLabel7").bothE("linkLabel7")
            .otherV().hasLabel("objLabel8").limit(5).path()).toList();
        for(Object path : paths){
            System.out.println(path);
        }
        /*
        bothE("linkLabel1")
                .otherV().hasLabel("objLabel2").bothE("linkLabel2")
                .otherV().hasLabel("objLabel3").bothE("linkLabel3")
                .otherV().hasLabel("objLabel4").bothE("linkLabel4")
                .otherV().hasLabel("objLabel5").limit(5).path()
        bothE("linkLabel5")
                .otherV().hasLabel("objLabel6").bothE("linkLabel6")
                .otherV().hasLabel("objLabel7").bothE("linkLabel7")
                .otherV().hasLabel("objLabel8").limit(5).path()
         */
        LOGGER.info(String.format("结果条数%s,用时%s",paths.size(),started.elapsed(TimeUnit.MILLISECONDS)));
    }



    @Test
    public void select5(){
        Stopwatch started = Stopwatch.createStarted();
        List<Path> paths = g.V("A0_15_000").hasLabel("objLabel1").and(bothE("linkLabel1")
                .otherV().hasLabel("objLabel2").bothE("linkLabel2")
                .otherV().hasLabel("objLabel3").bothE("linkLabel3")
                .otherV().hasLabel("objLabel4").bothE("linkLabel4")
                .otherV().hasLabel("objLabel5").limit(5).path(), bothE("linkLabel5")
                .otherV().hasLabel("objLabel6").bothE("linkLabel6")
                .otherV().hasLabel("objLabel7").bothE("linkLabel7")
                .otherV().hasLabel("objLabel9").limit(5).path())

            .union(bothE("linkLabel1")
            .otherV().hasLabel("objLabel2").bothE("linkLabel2")
            .otherV().hasLabel("objLabel3").bothE("linkLabel3")
            .otherV().hasLabel("objLabel4").bothE("linkLabel4")
            .otherV().hasLabel("objLabel5").limit(5).path(), bothE("linkLabel5")
            .otherV().hasLabel("objLabel6").bothE("linkLabel6")
            .otherV().hasLabel("objLabel7").bothE("linkLabel7")
            .otherV().hasLabel("objLabel8").limit(5).path()).toList();
        for(Object path : paths){
            System.out.println(path);
        }
        /*
        bothE("linkLabel1")
                .otherV().hasLabel("objLabel2").bothE("linkLabel2")
                .otherV().hasLabel("objLabel3").bothE("linkLabel3")
                .otherV().hasLabel("objLabel4").bothE("linkLabel4")
                .otherV().hasLabel("objLabel5").limit(5).path()
        bothE("linkLabel5")
                .otherV().hasLabel("objLabel6").bothE("linkLabel6")
                .otherV().hasLabel("objLabel7").bothE("linkLabel7")
                .otherV().hasLabel("objLabel8").limit(5).path()
         */
        LOGGER.info(String.format("结果条数%s,用时%s",paths.size(),started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void select6(){
        Stopwatch started = Stopwatch.createStarted();
        List<Path> paths = g.V("A0_15_000").hasLabel("objLabel1").and(bothE("linkLabel1")
            .otherV().hasLabel("objLabel2").bothE("linkLabel2")
            .otherV().hasLabel("objLabel3").and(bothE("linkLabel3")
            .otherV().hasLabel("objLabel4").bothE("linkLabel4")
            .otherV().hasLabel("objLabel5").limit(5),bothE("linkLabel9")
                .otherV().hasLabel("objLabel0").limit(5)), bothE("linkLabel5")
            .otherV().hasLabel("objLabel6").bothE("linkLabel6")
            .otherV().hasLabel("objLabel7").bothE("linkLabel7")
            .otherV().hasLabel("objLabel8").limit(5)).union(bothE("linkLabel1")
            .otherV().hasLabel("objLabel2").bothE("linkLabel2")
            .otherV().hasLabel("objLabel3").union(bothE("linkLabel3")
                    .otherV().hasLabel("objLabel4").bothE("linkLabel4")
                    .otherV().hasLabel("objLabel5").limit(5),bothE("linkLabel9")
                    .otherV().hasLabel("objLabel0").limit(5))
            , bothE("linkLabel5")
            .otherV().hasLabel("objLabel6").bothE("linkLabel6")
            .otherV().hasLabel("objLabel7").bothE("linkLabel7")
            .otherV().hasLabel("objLabel8").limit(5)).path().toList();
        for(Object path : paths){
            System.out.println(path);
        }
        /*
        bothE("linkLabel1")
                .otherV().hasLabel("objLabel2").bothE("linkLabel2")
                .otherV().hasLabel("objLabel3").bothE("linkLabel3")
                .otherV().hasLabel("objLabel4").bothE("linkLabel4")
                .otherV().hasLabel("objLabel5").limit(5).path()
        bothE("linkLabel5")
                .otherV().hasLabel("objLabel6").bothE("linkLabel6")
                .otherV().hasLabel("objLabel7").bothE("linkLabel7")
                .otherV().hasLabel("objLabel8").limit(5).path()
         */
        LOGGER.info(String.format("结果条数%s,用时%s",paths.size(),started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void select6or(){
        Stopwatch started = Stopwatch.createStarted();
        List<Path> paths = g.V("A0_15_000").hasLabel("objLabel1").union(bothE("linkLabel1")
                .otherV().hasLabel("objLabel2").bothE("linkLabel2")
                .otherV().hasLabel("objLabel3").union(bothE("linkLabel3")
                    .otherV().hasLabel("objLabel4").bothE("linkLabel4")
                    .otherV().hasLabel("objLabel5").limit(5),bothE("linkLabel9")
                    .otherV().hasLabel("objLabel0").limit(5))
            , bothE("linkLabel5")
                .otherV().hasLabel("objLabel6").bothE("linkLabel6")
                .otherV().hasLabel("objLabel7").bothE("linkLabel7")
                .otherV().hasLabel("objLabel8").limit(5)).path().toList();
        for(Object path : paths){
            System.out.println(path);
        }
        /*
        bothE("linkLabel1")
                .otherV().hasLabel("objLabel2").bothE("linkLabel2")
                .otherV().hasLabel("objLabel3").bothE("linkLabel3")
                .otherV().hasLabel("objLabel4").bothE("linkLabel4")
                .otherV().hasLabel("objLabel5").limit(5).path()
        bothE("linkLabel5")
                .otherV().hasLabel("objLabel6").bothE("linkLabel6")
                .otherV().hasLabel("objLabel7").bothE("linkLabel7")
                .otherV().hasLabel("objLabel8").limit(5).path()
         */
        LOGGER.info(String.format("结果条数%s,用时%s",paths.size(),started.elapsed(TimeUnit.MILLISECONDS)));
    }

    public void select6or1(){
        Stopwatch started = Stopwatch.createStarted();
        List<Path> paths = g.V("A0_15_000").hasLabel("objLabel1").and(bothE("linkLabel1")
            .otherV().hasLabel("objLabel2").bothE("linkLabel2")
            .otherV().hasLabel("objLabel3").or(bothE("linkLabel3")
                .otherV().hasLabel("objLabel4").bothE("linkLabel4")
                .otherV().hasLabel("objLabel5").limit(5),bothE("linkLabel9")
                .otherV().hasLabel("objLabel0").limit(5)), bothE("linkLabel5")
            .otherV().hasLabel("objLabel6").bothE("linkLabel6")
            .otherV().hasLabel("objLabel7").bothE("linkLabel7")
            .otherV().hasLabel("objLabel8").limit(5)).union(bothE("linkLabel1")
                .otherV().hasLabel("objLabel2").bothE("linkLabel2")
                .otherV().hasLabel("objLabel3").union(bothE("linkLabel3")
                    .otherV().hasLabel("objLabel4").bothE("linkLabel4")
                    .otherV().hasLabel("objLabel5").limit(5),bothE("linkLabel9")
                    .otherV().hasLabel("objLabel0").limit(5))
            , bothE("linkLabel5")
                .otherV().hasLabel("objLabel6").bothE("linkLabel6")
                .otherV().hasLabel("objLabel7").bothE("linkLabel7")
                .otherV().hasLabel("objLabel8").limit(5)).path().toList();
        for(Object path : paths){
            System.out.println(path);
        }
        /*
        bothE("linkLabel1")
                .otherV().hasLabel("objLabel2").bothE("linkLabel2")
                .otherV().hasLabel("objLabel3").bothE("linkLabel3")
                .otherV().hasLabel("objLabel4").bothE("linkLabel4")
                .otherV().hasLabel("objLabel5").limit(5).path()
        bothE("linkLabel5")
                .otherV().hasLabel("objLabel6").bothE("linkLabel6")
                .otherV().hasLabel("objLabel7").bothE("linkLabel7")
                .otherV().hasLabel("objLabel8").limit(5).path()
         */
        LOGGER.info(String.format("结果条数%s,用时%s",paths.size(),started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void select6orProfile(){
        Stopwatch started = Stopwatch.createStarted();
        List<TraversalMetrics> paths = g.V("A0_15_000").hasLabel("objLabel1").and(bothE("linkLabel1")
            .otherV().hasLabel("objLabel2").bothE("linkLabel2")
            .otherV().hasLabel("objLabel3").or(bothE("linkLabel3")
                .otherV().hasLabel("objLabel4").bothE("linkLabel4")
                .otherV().hasLabel("objLabel5").limit(5), bothE("linkLabel9")
                .otherV().hasLabel("objLabel0").limit(5)), bothE("linkLabel5")
            .otherV().hasLabel("objLabel6").bothE("linkLabel6")
            .otherV().hasLabel("objLabel7").bothE("linkLabel7")
            .otherV().hasLabel("objLabel8").limit(5)).union(bothE("linkLabel1")
                .otherV().hasLabel("objLabel2").bothE("linkLabel2")
                .otherV().hasLabel("objLabel3").union(bothE("linkLabel3")
                    .otherV().hasLabel("objLabel4").bothE("linkLabel4")
                    .otherV().hasLabel("objLabel5").limit(5), bothE("linkLabel9")
                    .otherV().hasLabel("objLabel0").limit(5))
            , bothE("linkLabel5")
                .otherV().hasLabel("objLabel6").bothE("linkLabel6")
                .otherV().hasLabel("objLabel7").bothE("linkLabel7")
                .otherV().hasLabel("objLabel8").limit(5)).path().profile().toList();
        for(Object path : paths){
            System.out.println(path);
        }
        /*
        bothE("linkLabel1")
                .otherV().hasLabel("objLabel2").bothE("linkLabel2")
                .otherV().hasLabel("objLabel3").bothE("linkLabel3")
                .otherV().hasLabel("objLabel4").bothE("linkLabel4")
                .otherV().hasLabel("objLabel5").limit(5).path()
        bothE("linkLabel5")
                .otherV().hasLabel("objLabel6").bothE("linkLabel6")
                .otherV().hasLabel("objLabel7").bothE("linkLabel7")
                .otherV().hasLabel("objLabel8").limit(5).path()
         */
        LOGGER.info(String.format("结果条数%s,用时%s",paths.size(),started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void select7(){
        KydsjTraversal<Vertex, Path> path1 = g.V("A0_15_000").hasLabel("objLabel1").and(bothE("linkLabel1")
            .otherV().hasLabel("objLabel2").bothE("linkLabel2")
            .otherV().hasLabel("objLabel3").and(bothE("linkLabel3")
                .otherV().hasLabel("objLabel4").bothE("linkLabel4")
                .otherV().hasLabel("objLabel5").limit(5), bothE("linkLabel9")
                .otherV().hasLabel("objLabel0").limit(5)), bothE("linkLabel5")
            .otherV().hasLabel("objLabel6").bothE("linkLabel6")
            .otherV().hasLabel("objLabel7").bothE("linkLabel7")
            .otherV().hasLabel("objLabel8").limit(5)).union(bothE("linkLabel1")
                .otherV().hasLabel("objLabel2").bothE("linkLabel2")
                .otherV().hasLabel("objLabel3").union(bothE("linkLabel3")
                    .otherV().hasLabel("objLabel4").bothE("linkLabel4")
                    .otherV().hasLabel("objLabel5").limit(5), bothE("linkLabel9")
                    .otherV().hasLabel("objLabel9").limit(5))
            , bothE("linkLabel5")
                .otherV().hasLabel("objLabel6").bothE("linkLabel6")
                .otherV().hasLabel("objLabel7").bothE("linkLabel7")
                .otherV().hasLabel("objLabel8").limit(5)).path();
        System.out.println(path1);
        /*
        bothE("linkLabel1")
                .otherV().hasLabel("objLabel2").bothE("linkLabel2")
                .otherV().hasLabel("objLabel3").bothE("linkLabel3")
                .otherV().hasLabel("objLabel4").bothE("linkLabel4")
                .otherV().hasLabel("objLabel5").limit(5).path()
        bothE("linkLabel5")
                .otherV().hasLabel("objLabel6").bothE("linkLabel6")
                .otherV().hasLabel("objLabel7").bothE("linkLabel7")
                .otherV().hasLabel("objLabel8").limit(5).path()
         */
    }

    @Test
    public void select8(){
        Stopwatch started = Stopwatch.createStarted();
        List<Path> paths  = g.V("00000f5e7c857986_15_000","000006f8e594a383_16_000")
            .union(bothE("link_phonecall").otherV().simplePath()
                .hasLabel("call").union(bothE().otherV().simplePath().union(bothE().otherV().simplePath())))
            .path().by(DefaultPropertyKey.TITLE.getKey()).by(DefaultPropertyKey.LINK_TEXT.getKey()).toList();
       /* Set<String> set = Sets.newHashSet();
        for(Path path :paths){
            List<Object> objects = path.objects();
            for(Object o : objects)
            {
                if(o instanceof Vertex)
                {
                    Vertex v = (Vertex)o;
                    String tid = v.id().toString();
                    if(set.contains(tid))
                    {
                        continue;
                    }
                    set.add(tid);
                    String text = v.property(DefaultPropertyKey.TITLE.getKey()).orElse("----").toString();
                    String label = v.label();
                }
                else if(o instanceof Edge)
                {
                    Edge e = (Edge)o;
                    String linkId = e.id().toString();
                    if(set.contains(linkId))
                    {
                        continue;
                    }
                    set.add(linkId);
                    String from = e.outVertex().id().toString();
                    String to = e.inVertex().id().toString();
                    String text = e.property(DefaultPropertyKey.LINK_TEXT.getKey()).orElse("").toString();
                    String label = e.label();
                }
            }
        }*/
        started.stop();
        LOGGER.info(String.format("结果条数%s,用时%s",paths.size(),started.elapsed(TimeUnit.MILLISECONDS)));

    }

    @Test
    public void select9() throws ExecutionException, InterruptedException {
        int thread=Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(thread,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("read-data-%d").build());//定义线程数
        Stopwatch started = Stopwatch.createStarted();
        List<Path> paths  = g.V("00000f5e7c857986_15_000","000006f8e594a383_16_000")
            .union(bothE("link_phonecall").otherV().simplePath()
                .hasLabel("call").union(bothE().otherV().simplePath().union(bothE().otherV().simplePath())))
            .path().toList();
        List<Future<Integer>> futures= Lists.newArrayList();
        List<List<Path>> partitionList = Lists.partition(paths, paths.size()/thread);
        for(List<Path> pathList:partitionList) {
            Future<Integer> submit = pool.submit(new MyRead(pathList));
            futures.add(submit);
        }
        long total=0;
        for(Future<Integer> future:futures){
            total+=future.get();
        }
        started.stop();
        LOGGER.info(String.format("结果条数%s,用时%s",paths.size(),started.elapsed(TimeUnit.MILLISECONDS)));

    }
    @Test
    public void select10() throws ExecutionException, InterruptedException {
        int thread=Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(thread,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("read-data-%d").build());//定义线程数
        Stopwatch started = Stopwatch.createStarted();
        List<Path> paths  = g.V("00000f5e7c857986_15_000","000006f8e594a383_16_000")
            .union(bothE("link_phonecall").otherV().simplePath()
                .hasLabel("call").union(bothE().otherV().simplePath().union(bothE().otherV().simplePath())))
            .path().toList();
        KgPathGraph result = new KgPathGraph();
        if(paths.size()>0) {
            List<Future<KgPathGraph>> futures = Lists.newArrayList();
            List<List<Path>> partitionList = Lists.partition(paths, paths.size() / thread);
            for (List<Path> pathList : partitionList) {
                Future<KgPathGraph> submit = pool.submit(new PathToKgPathGraph(pathList));
                futures.add(submit);
            }
            for (Future<KgPathGraph> future : futures) {
                KgPathGraph pathGraph = future.get();
                KgDiagram kgDiagram = pathGraph.getKgDiagram();
                KgDiagram diagram = result.getKgDiagram();
                diagram.getNodeDataArray().addAll(kgDiagram.getNodeDataArray());
                diagram.getLinkDataArray().addAll(kgDiagram.getLinkDataArray());
                result.getKgPaths().addAll(pathGraph.getKgPaths());
            }
        }
        started.stop();
        LOGGER.info(String.format("结果条数%s,用时%s",paths.size(),started.elapsed(TimeUnit.MILLISECONDS)));

    }

    @Test
    public void select11(){
        Stopwatch started = Stopwatch.createStarted();
        List<Path> paths = g.V("0323d1d3400e17bf_5_000")
            .or(__.union(bothE()).otherV().simplePath().hasId("53995766bb369f79_14_000"),__.union(bothE()).otherV().simplePath().hasId("6b4a99c26da6728b_16_000"))
            .union(bothE().otherV().simplePath().hasId("53995766bb369f79_14_000"),
                bothE().otherV().simplePath().hasId("6b4a99c26da6728b_16_000")).limit(5).path().toList();
        for(Object path : paths){
            System.out.println(path);
        }
        LOGGER.info(String.format("结果条数%s,用时%s",paths.size(),started.elapsed(TimeUnit.MILLISECONDS)));
    }
    static class MyRead implements Callable<Integer> {
        private List<Path> paths;
        public MyRead(List<Path> paths){
            this.paths=paths;
        }
        @Override
        public Integer call() throws Exception {
            Stopwatch started1 = Stopwatch.createStarted();
            Set<String> set = Sets.newHashSet();
            for(Path path :paths){
                List<Object> objects = path.objects();
                for(Object o : objects)
                {
                    if(o instanceof Vertex)
                    {
                        Vertex v = (Vertex)o;
                        String tid = v.id().toString();
                        if(set.contains(tid))
                        {
                            continue;
                        }
                        set.add(tid);
                        String label = v.label();
                        String text = v.property(DefaultPropertyKey.TITLE.getKey()).orElse("----").toString();
                    }
                    else if(o instanceof Edge)
                    {
                        Edge e = (Edge)o;
                        String linkId = e.id().toString();
                        if(set.contains(linkId))
                        {
                            continue;
                        }
                        set.add(linkId);
                        String from = e.outVertex().id().toString();
                        String to = e.inVertex().id().toString();
                        String text = e.property(DefaultPropertyKey.LINK_TEXT.getKey()).orElse("").toString();
                        //String left_type = e.property(DefaultPropertyKey.LEFT_TYPE.getKey()).orElse("").toString();
                        //String right_type = e.property(DefaultPropertyKey.RIGHT_TYPE.getKey()).orElse("").toString();
                        String label = e.label();
                    }
                }
            }
            started1.stop();
            LOGGER.info(String.format("当前线程%s,已经处理了->%s条用时%s", Thread.currentThread().getName(), paths.size(),
                started1.elapsed(TimeUnit.MILLISECONDS)));
            return paths.size();
        }
    }

}
