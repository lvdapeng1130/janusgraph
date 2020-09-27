package org.janusgraph.kydsj;

import org.apache.janusgraph.spark.computer.SparkJanusGraphComputer;
import org.apache.janusgraph.spark.mapreduce.CombineObjectByConditionMapper;
import org.apache.janusgraph.spark.mapreduce.CombineObjectMakerMapper;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComputeMergeGraph {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComputeMergeGraph.class);
    private final static String PROPERTY_FILE_PATH = "D:/github/janusgraph/janusgraph-kydsj/src/main/resources/read-hbase-janus-local.properties";

    public static void main(String[] args) throws Exception {
        //System.setProperty("user.name", "hadoop");
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        //这个配置非常重要，这个指定一个HDFS路径，路径中存放jar包为JanusGraph安装目录解压后的依赖目录 janusgraph/lib
        //将目录中的jar包上传到hdfs上，任务运行时需要的依赖
        //System.setProperty("HADOOP_GREMLIN_LIBS", "hdfs://192.168.1.47:8020/user/hadoop/janusgraph/spark-yarn/");
        StandardJanusGraph graph = (StandardJanusGraph)JanusGraphFactory.open(PROPERTY_FILE_PATH);
        LOGGER.info(String.format("driver当前uuid->%s",graph.getUniqueInstanceId()));
        //SparkJanusGraphComputer sparkJanusGraphComputer=new SparkJanusGraphComputer((StandardJanusGraph)graph);
        //ComputerResult computerResult = sparkJanusGraphComputer.mapReduce(new CounterMapper()).submit().get();
        ComputerResult computerResult = graph.compute(SparkJanusGraphComputer.class)
            .mapReduce(new CombineObjectMakerMapper()).mapReduce(new CombineObjectByConditionMapper())
            .submit().get();
        //GraphTraversalSource g = graph.traversal().withComputer(SparkGraphComputer.class);

       /* long count = g.V().count().next();
        LOGGER.info(count + "-----g.V().count()-------------------------------");
        long count_E = g.E().count().next();
        LOGGER.info(count_E + "-----g.E().count()-------------------------------");*/
        graph.close();
        computerResult.graph().close();
        System.out.println(2222);
    }

}