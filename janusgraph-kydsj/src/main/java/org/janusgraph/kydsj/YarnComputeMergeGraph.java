package org.janusgraph.kydsj;

import org.apache.janusgraph.spark.computer.SparkJanusGraphComputer;
import org.apache.janusgraph.spark.mapreduce.CombineObjectByConditionMapper;
import org.apache.janusgraph.spark.mapreduce.CombineObjectMakerMapper;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.hadoop.formats.hbase.HBaseInputFormat;
import org.janusgraph.hadoop.serialize.JanusGraphKryoRegistrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YarnComputeMergeGraph {

    private static final Logger LOGGER = LoggerFactory.getLogger(YarnComputeMergeGraph.class);
    private final static String PROPERTY_FILE_PATH = "D:/github/janusgraph/janusgraph-kydsj/src/main/resources/combine-graph-yarn.properties";

    public static void main(String[] args) throws Exception {
        //System.setProperty("user.name", "hadoop");
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        //这个配置非常重要，这个指定一个HDFS路径，路径中存放jar包为JanusGraph安装目录解压后的依赖目录 janusgraph/lib
        //将目录中的jar包上传到hdfs上，任务运行时需要的依赖
        System.setProperty("HADOOP_GREMLIN_LIBS", "hdfs://192.168.1.47:8020/user/hadoop/janusgraph/spark-janusgraph/");
        StandardJanusGraph graph = (StandardJanusGraph)JanusGraphFactory.open(PROPERTY_FILE_PATH);
        LOGGER.info(String.format("driver当前uuid->%s",graph.getUniqueInstanceId()));
        SparkJanusGraphComputer sparkJanusGraphComputer = graph.compute(SparkJanusGraphComputer.class);
        ComputerResult computerResult =sparkJanusGraphComputer
            .master("yarn")
            .serializer(KryoSerializer.class)
            .sparkKryoRegistrator(JanusGraphKryoRegistrator.class)
            .configure(SparkLauncher.DEPLOY_MODE, "client")
            .configure(SparkLauncher.EXECUTOR_MEMORY, "6g")
            .configure(SparkLauncher.DRIVER_MEMORY, "1g")
            .configure(SparkLauncher.EXECUTOR_CORES, 3)
            .configure("spark.yarn.jars", "hdfs://192.168.1.47:8020/user/hadoop/janusgraph/spark_jars/*.jar")
            .configure("spark.task.cpus", 1)
            .configure("spark.executor.instances", 30)
            .configure("spark.default.parallelism", 270)
            .configure("spark.driver.host", "192.168.0.123")
            .configure(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, "-Xss20m")
            .configure(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS, "-Xss20m")
            .configure(Constants.GREMLIN_HADOOP_GRAPH_READER, HBaseInputFormat.class.getName())
            .configure(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GryoOutputFormat.class.getName())
            .configure(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, true)
            .configure(Constants.GREMLIN_HADOOP_INPUT_LOCATION, "none")
            .configure(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, "output")
            .configure(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true)

            //---------------------设置合并条件-------------------
            //设置要参与处理的对象类型
            .combineConditionObjectType("object_qqqun")
            //按照指定属性的值相同进行合并
            .combineConditionPropertyType("qqqun_num")
            //在生成mapreduce的key时是否考虑对象类型
            .combineConditionThinkOverObjectType(true)
            //在处理被合并对象属性类型时忽略的属性类型，设置了此值的属性类型的值将不会拷贝到合并对象上
            .combineEliminatePropertyType("tid")
            //在处理被合并对象关系类型时忽略的关系类型，设置了此值的关系类型将不会拷贝到合并对象上
            //.combineEliminateLinkType("")

            .mapReduce(new CombineObjectMakerMapper())
            .mapReduce(new CombineObjectByConditionMapper())
            .submit().get();

        //GraphTraversalSource g = graph.traversal().withComputer(SparkGraphComputer.class);
       /* long count = g.V().count().next();
        LOGGER.info(count + "-----g.V().count()-------------------------------");
        long count_E = g.E().count().next();
        LOGGER.info(count_E + "-----g.E().count()-------------------------------");*/
        computerResult.graph().close();
        graph.close();
        System.out.println(2222);
    }
}