package org.janusgraph.kydsj;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphComputeSelectApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphComputeSelectApp.class);
    private final static String PROPERTY_FILE_PATH = "D:/github/janusgraph/janusgraph-kydsj/src/main/resources/read-hbase-local.properties";

    public static void main(String[] args) throws Exception {
        //System.setProperty("user.name", "hadoop");
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        //这个配置非常重要，这个指定一个HDFS路径，路径中存放jar包为JanusGraph安装目录解压后的依赖目录 janusgraph/lib
        //将目录中的jar包上传到hdfs上，任务运行时需要的依赖
        //System.setProperty("HADOOP_GREMLIN_LIBS", "hdfs://192.168.1.47:8020/user/hadoop/janusgraph/spark-yarn/");
        Graph graph = GraphFactory.open(PROPERTY_FILE_PATH);
        GraphTraversalSource g = graph.traversal().withComputer(SparkGraphComputer.class);
        long count = g.V().hasLabel("object_qqqun").has("qqqun_num","0").count().next();
        LOGGER.info(count + "统计条数-->QQ群号是0的-----g.V().count()-------------------------------");
        long count_E = g.E().count().next();
        LOGGER.info(count_E + "统计条数-->总共关系-----g.E().count()-------------------------------");
        long countVV = g.V().hasLabel("object_qqqun").has("qqqun_num","0").bothE("link_simple").count().next();
        LOGGER.info(countVV + "统计条数-->-----QQ群0的关系-------------------------------");

        graph.close();
    }

}