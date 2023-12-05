package org.janusgraph.kydsj;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.janusgraph.spark.structure.io.gryo.GryoRegistrator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.janusgraph.graphdb.util.Constants;
import org.janusgraph.hadoop.MapReduceIndexJobs;
import org.janusgraph.hadoop.formats.hbase.KYHBaseInputFormat;
import org.janusgraph.util.system.ConfigurationUtil;
import org.janusgraph.util.system.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class IndexRepairJob {

    private static Logger log= LoggerFactory.getLogger(IndexRepairJob.class);
    public static final String STORAGE_HBASE_TABLE="storage.hbase.table";
    public static final String GRAPH_ZOOKEEPER_GRAPH_NODE="graph.zookeeper-graph-node";
    public static final String INDEX_SEARCH_INDEX_NAME="index.search.index-name";
    public static final String INDEX_SEARCH_HOSTNAME="index.search.hostname";
    public static final String GRAPH_CONFIG_NAME="trsgraph-bulkload.properties";

    private static Options defineOptions(){
        Options options = new Options();
        options.addOption("h", "help", false, "参数帮助");
        Option option=new Option("graphName", "graphName", true, "图库名称");
        option.setRequired(true);
        options.addOption(option);
        options.addOption("indexName", "indexName", false, "索引名称");
        return options;
    }

    private static void printHelpInfo(Options options, CommandLine cmd) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        // 这里显示简短的帮助信息
        hf.printHelp("testApp", options, true);
        // 打印opts的名称和值
        System.out.println("--------------------------------------");
        Option[] opts = cmd.getOptions();
        if (opts != null) {
            for (Option opt1 : opts) {
                String name = opt1.getLongOpt();
                String value = cmd.getOptionValue(name);
                System.out.println(name + "=>" + value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            Options options = IndexRepairJob.defineOptions();
            CommandLineParser parser = new PosixParser();
            CommandLine cmd = parser.parse(options, args);
            if(cmd.hasOption("h")) {
                printHelpInfo(options, cmd);
                System.exit(1);
            }else{
                String graphName = cmd.getOptionValue("graphName");
                String indexName = cmd.getOptionValue("indexName");
                InputStream stream=IndexRepairJob.class.getResourceAsStream("/"+ GRAPH_CONFIG_NAME);
                try {
                    if(stream!=null) {
                        Properties properties = new Properties();
                        properties.load(stream);
                        properties.setProperty(GRAPH_ZOOKEEPER_GRAPH_NODE,graphName.trim());
                        properties.setProperty(STORAGE_HBASE_TABLE,graphName.trim());
                        properties.setProperty(INDEX_SEARCH_INDEX_NAME,graphName.trim());
                        Map<String, Object> propertieMap=(Map)properties;
                        MapConfiguration configuration = ConfigurationUtil.loadMapConfiguration(propertieMap);
                        if (!configuration.containsKey(org.apache.tinkerpop.gremlin.hadoop.Constants.SPARK_SERIALIZER)) {
                            configuration.setProperty(org.apache.tinkerpop.gremlin.hadoop.Constants.SPARK_SERIALIZER, KryoSerializer.class.getCanonicalName());
                            if (!configuration.containsKey(org.apache.tinkerpop.gremlin.hadoop.Constants.SPARK_KRYO_REGISTRATOR))
                                configuration.setProperty(org.apache.tinkerpop.gremlin.hadoop.Constants.SPARK_KRYO_REGISTRATOR, GryoRegistrator.class.getCanonicalName());
                        }
                        //final Configuration hadoopConfiguration = ConfUtil.makeHadoopConfiguration(configuration);
                        final Configuration hadoopConfiguration = makeHadoopConfiguration(configuration);
                        hadoopConfiguration.set(Constants.GREMLIN_HADOOP_GRAPH_VERTEXLABELS,"person");
                        hadoopConfiguration.set(Constants.GREMLIN_HADOOP_GRAPH_EDGELABELS,"link_havea");
                        MapReduceIndexJobs.copyPropertiesToInputAndOutputConf(hadoopConfiguration, properties);
                        SparkSession spark = SparkSession
                                .builder().master("local[1]")
                                .appName("JavaSparkPi")
                                .getOrCreate();
                        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
                        JavaRDD<VertexWritable> graphRDD = (JavaRDD<VertexWritable>)jsc.newAPIHadoopRDD(hadoopConfiguration,
                                KYHBaseInputFormat.class,
                                NullWritable.class,
                                VertexWritable.class).map(tuple->tuple._2);
                        graphRDD.foreachPartition(new IndexRepairJobProcessor(propertieMap,indexName));
                    }else{
                        log.error(String.format("读取图库配置文件失败：%s",GRAPH_CONFIG_NAME));
                    }
                } finally {
                    if(stream!=null) {
                        IOUtils.closeQuietly(stream);
                    }
                }
            }
        }catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }

    public static Configuration makeHadoopConfiguration(final org.apache.commons.configuration2.Configuration apacheConfiguration) {
        Configuration hadoopConfiguration = new Configuration();
        apacheConfiguration.getKeys().forEachRemaining((key) -> {
            Object object = apacheConfiguration.getProperty(key);
            if(object instanceof List) {
                List list=(List)object;
                String collect = (String)list.stream().collect(Collectors.joining(","));
                hadoopConfiguration.set(key, collect);
            }else{
                hadoopConfiguration.set(key, object.toString());
            }
        });
        return hadoopConfiguration;
    }
}
