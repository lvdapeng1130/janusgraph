package org.janusgraph.kydsj;

import org.apache.janusgraph.spark.computer.SparkJanusGraphComputer;
import org.apache.janusgraph.spark.mapreduce.CombineObjectByConditionMapper;
import org.apache.janusgraph.spark.mapreduce.CombineObjectMakerMapper;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.idassigner.Stopwatch;
import org.janusgraph.hadoop.formats.hbase.HBaseInputFormat;
import org.janusgraph.hadoop.serialize.JanusGraphKryoRegistrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ComputeMergeGraph {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComputeMergeGraph.class);
    private final static String PROPERTY_FILE_PATH = "D:/github/janusgraph/janusgraph-kydsj/src/main/resources/read-hbase-janus-local.properties";
    private final static String PROPERTY_FILE_PATH_2 = "D:/github/janusgraph/janusgraph-kydsj/src/main/resources/read-hbase-janus-local-y.properties";

    public static void main(String[] args) throws Exception {
        //System.setProperty("user.name", "hadoop");
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        //这个配置非常重要，这个指定一个HDFS路径，路径中存放jar包为JanusGraph安装目录解压后的依赖目录 janusgraph/lib
        //将目录中的jar包上传到hdfs上，任务运行时需要的依赖
        //System.setProperty("HADOOP_GREMLIN_LIBS", "hdfs://192.168.1.47:8020/user/hadoop/janusgraph/spark-yarn/");
        f2();
    }

    private static void f1() throws Exception {
        StandardJanusGraph graph = (StandardJanusGraph)JanusGraphFactory.open(PROPERTY_FILE_PATH);
        LOGGER.info(String.format("driver当前uuid->%s",graph.getUniqueInstanceId()));

        //SparkJanusGraphComputer sparkJanusGraphComputer=new SparkJanusGraphComputer((StandardJanusGraph)graph);
        //ComputerResult computerResult = sparkJanusGraphComputer.mapReduce(new CounterMapper()).submit().get();
        ComputerResult computerResult = graph.compute(SparkJanusGraphComputer.class)
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
            .persist(GraphComputer.Persist.NOTHING)
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

    private static void f2() throws Exception {
        Stopwatch started = Stopwatch.createStarted();
        Stopwatch started1 = Stopwatch.createStarted();
        StandardJanusGraph graph = (StandardJanusGraph)JanusGraphFactory.open(PROPERTY_FILE_PATH_2);
        LOGGER.info(String.format("driver当前uuid->%s,打开一次图库用时%s",graph.getUniqueInstanceId(),started1.elapsed(TimeUnit.MILLISECONDS)));
        SparkJanusGraphComputer sparkJanusGraphComputer = graph.compute(SparkJanusGraphComputer.class);
        ComputerResult computerResult =sparkJanusGraphComputer
            .master("local[*]")
            .serializer(KryoSerializer.class)
            .sparkKryoRegistrator(JanusGraphKryoRegistrator.class)
            .configure(SparkLauncher.EXECUTOR_MEMORY, "6g")
            .configure(SparkLauncher.DRIVER_MEMORY, "1g")
            .configure(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, "-Xss20m")
            .configure(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS, "-Xss20m")
            .configure(Constants.GREMLIN_HADOOP_GRAPH_READER, HBaseInputFormat.class.getName())
            .configure(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GryoOutputFormat.class.getName())
            .configure(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, true)
            .configure(Constants.GREMLIN_HADOOP_INPUT_LOCATION, "none")
            .configure(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, "output")
            .configure(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, false)
            
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
            .vertices(__.hasLabel("object_qqqun"))

            .mapReduce(new CombineObjectMakerMapper())
            .mapReduce(new CombineObjectByConditionMapper())
            .submit().get();
        computerResult.graph().close();
        graph.close();
        started.stop();
        System.out.println("用时"+started.elapsed(TimeUnit.MILLISECONDS));
    }

}
