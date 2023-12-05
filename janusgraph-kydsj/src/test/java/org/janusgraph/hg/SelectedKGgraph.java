package org.janusgraph.hg;

import com.google.common.base.Stopwatch;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.dsl.KydsjTraversal;
import org.janusgraph.dsl.KydsjTraversalSource;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

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
        List<Map<String, Object>> result = g.V().as("n_0")
            .hasLabel("gr_e2")
            .outE("link_cz_e2").inV().hasLabel("hb_e2")
            .outE("link_mdjc_e2").inV().hasLabel("ka_e2").path()
            .from("n_0").as("p").project("p").by(__.identity()).limit(20)
            .project("p").toList();
        started.stop();
        LOGGER.info(String.format("结果条数%s,用时%s",result.size(),started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void select3(){
        Stopwatch started = Stopwatch.createStarted();
        List<Map<String, Object>> result = g.V().as("n_0").hasLabel("qy_e3")
            .bothE().dedup().by(__.path()).as("r_1").otherV()
            .bothE().dedup().by(__.path()).as("r_2").otherV()
            .bothE().dedup().by(__.path()).as("r_3")
            .otherV().hasLabel("qy_e3").path().from("n_0").as("p").where(__.select("r_1")
                .where(P.neq("r_3"))).select("p").project("p").by(__.identity())
            .limit(20).project("p").toList();
        LOGGER.info(String.format("结果条数%s,用时%s",result.size(),started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void select4(){
        Stopwatch started = Stopwatch.createStarted();
        List<Map<String, Object>> result = g.V().as("n_0").hasLabel("qy_e4")
            .inE("link_scxsdw_e4").outV().hasLabel("hw_e4")
            .has("bgdbh_e4", P.eq("303694351")).outE("link_zscqqq_e4")
            .inV().hasLabel("zscqbaxx_e4")
            .outE("link_qlr_e4").inV().hasLabel("qy_e4").path().from("n_0")
            .as("p").project("p").by(__.identity()).limit(100).project("p").toList();
        LOGGER.info(String.format("结果条数%s,用时%s",result.size(),started.elapsed(TimeUnit.MILLISECONDS)));
    }
    @Test
    public void select4_1(){
        Stopwatch started = Stopwatch.createStarted();
        List<Map<String, Object>> result = g.V().as("n_0").hasLabel("hw_e4")
            .has("bgdbh_e4", P.eq("303694351")).outE("link_scxsdw_e4")
            .inV().hasLabel("qy_e4").path().from("n_0")
            .as("p").project("p").by(__.identity()).limit(100).project("p").toList();
        LOGGER.info(String.format("结果条数%s,用时%s",result.size(),started.elapsed(TimeUnit.MILLISECONDS)));
    }
    @Test
    public void select4_2(){
        Stopwatch started = Stopwatch.createStarted();
        List<Map<String, Object>> result = g.V().as("n_0").hasLabel("hw_e4")
            .has("bgdbh_e4", P.eq("303694351")).outE("link_zscqqq_e4")
            .inV().hasLabel("zscqbaxx_e4")
            .outE("link_qlr_e4").inV().hasLabel("qy_e4").path().from("n_0")
            .as("p").project("p").by(__.identity()).limit(100).project("p").toList();
        LOGGER.info(String.format("结果条数%s,用时%s",result.size(),started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void select5(){
        Stopwatch started = Stopwatch.createStarted();
        List<Map<String, Object>> result = g.V().as("n_0").hasLabel("hw_e5")
            .has("spbhhs_e5", P.eq("147683885"))
            .has("bgdbh_e5", P.eq("176575041"))
            .inE("link_sbhw_e5").outV()
            .hasLabel("bgd_e5").outE("link_zyg_e5").inV()
            .hasLabel("ka_e5").path().from("n_0").as("p")
            .project("p").by(__.identity()).limit(100).project("p").toList();
        LOGGER.info(String.format("结果条数%s,用时%s",result.size(),started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void select5_other(){
        Stopwatch started = Stopwatch.createStarted();
        List<Map<String, Object>> result = g.V().as("n_0").hasLabel("hw_e5")
            .has("spbhhs_e5", P.eq("111111111111"))
            .has("bgdbh_e5", P.eq("2222222222222"))
            .outE("link_sbhw_e5").inV()
            .hasLabel("bgd_e5").outE("link_zyg_e5").inV()
            .hasLabel("ka_e5").path().from("n_0").as("p")
            .project("p").by(__.identity()).limit(100).project("p").toList();
        LOGGER.info(String.format("结果条数%s,用时%s",result.size(),started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void select6(){
        Stopwatch started = Stopwatch.createStarted();
        List<Vertex> result = g.V().hasLabel("hb_e6").has("jcj_e6", "E").limit(20).toList();
        LOGGER.info(String.format("结果条数%s,用时%s",result.size(),started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void select7(){
        Stopwatch started = Stopwatch.createStarted();
        List<Vertex> result = g.V().hasLabel("hb_e7").has("hbrq_e7", P.gt("2021-01-01"))
            .has("hbrq_e7", P.lt("2021-10-01")).limit(20).toList();
        LOGGER.info(String.format("结果条数%s,用时%s",result.size(),started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void drop1(){
        Stopwatch started = Stopwatch.createStarted();
        KydsjTraversal<Vertex, Vertex> hw_e1 = g.V().hasLabel("hw_e1");
        int size=0;
        while (hw_e1.hasNext()){
            Vertex next = hw_e1.next();
            try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
                .consistencyChecks(true)
                .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
                GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal(KydsjTraversalSource.class)
                    .V(next.id());
                Optional<Vertex> vertex = qqTraversal.tryNext();
                if(vertex.isPresent()){
                    Vertex vertex1 = vertex.get();
                    vertex1.remove();
                }
                threadedTx.commit();
            }
            size++;
        }
        LOGGER.info(String.format("结果条数%s,用时%s",size,started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void sel(){
        Stopwatch started = Stopwatch.createStarted();
        //List<Vertex> hw_e1 = g.V().hasLabel("hw_e1").has("spbhhs", P.eq("016324466")).limit(10).toList();
        List<Vertex> hw_e1 = g.V().hasLabel("hw_e1").limit(10).toList();
        LOGGER.info(String.format("结果条数%s,用时%s",hw_e1.size(),started.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void date(){
        Date date=new Date();
        System.out.println(date);
    }

    @Test
    public void testA() throws Exception{
        String dateStr = "2019-07-15 08:00:00";
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // 解析字符串，时区：东八区
        Date date = dateFormat.parse(dateStr);
        System.out.println(date.getTime());

        // 格式化日期，时区：0时区
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        System.out.println(dateFormat.format(date));
    }

}
