package org.janusgraph.graphdb.hbase;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.janusgraph.core.*;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.hbase.HBaseStoreManager;
import org.janusgraph.graphdb.JanusGraphBaseTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.idassigner.placement.SimpleBulkPlacementStrategy;
import org.janusgraph.testutil.TestGraphConfigs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author: ldp
 * @time: 2020/9/14 13:42
 * @jira:
 */
public class KGGraphOLAPTest extends JanusGraphBaseTest {

    private static final Random random = new Random();
    private static final Logger log =
        LoggerFactory.getLogger(KGGraphOLAPTest.class);

    @Override
    public WriteConfiguration getConfiguration() {
        String tableName="olap_test1";
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.JANUSGRAPH_ZOOKEEPER_URI, "192.168.1.47:2181,192.168.1.48:2181,192.168.1.49:2181");
        config.set(GraphDatabaseConfiguration.JANUSGRAPH_ZOOKEEPER_NAMESPACE, "trs-graph");
        config.set(GraphDatabaseConfiguration.GRAPH_NODE, tableName);
        config.set(GraphDatabaseConfiguration.ZOOKEEPER_SESSIONTIMEOUTMS, 5000);
        config.set(GraphDatabaseConfiguration.ZOOKEEPER_CONNECTIONTIMEOUTMS, 5000);
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "hbase");
        if (!StringUtils.isEmpty(tableName)) config.set(HBaseStoreManager.HBASE_TABLE, tableName);
        config.set(GraphDatabaseConfiguration.TIMESTAMP_PROVIDER, HBaseStoreManager.PREFERRED_TIMESTAMPS);
        config.set(SimpleBulkPlacementStrategy.CONCURRENT_PARTITIONS, 1);
        config.set(GraphDatabaseConfiguration.DROP_ON_CLEAR, false);
        //config.set(HBaseStoreManager.SHORT_CF_NAMES,false);
        // config.set(HBaseStoreManager.REGION_COUNT,10);
        config.set(GraphDatabaseConfiguration.STORAGE_HOSTS,new String[]{"192.168.1.47,192.168.1.48,192.168.1.49"});
        config.set(GraphDatabaseConfiguration.STORAGE_PORT,2181);
        //config.set(GraphDatabaseConfiguration.ALLOW_SETTING_VERTEX_ID,true);
        //config.set(GraphDatabaseConfiguration.IDAUTHORITY_WAIT, Duration.ofMillis(300000000L));
        return config.getConfiguration();
    }

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        this.testInfo = testInfo;
        this.config = getConfiguration();
        TestGraphConfigs.applyOverrides(config);
        Preconditions.checkNotNull(config);
        logManagers = new HashMap<>();
        readConfig = new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS, config, BasicConfiguration.Restriction.NONE);
        open(config);
    }

    @Test
    public void clearData() throws BackendException {
        this.config = getConfiguration();
        clearGraph(config);
    }

    @Test
    public void select(){
        Iterator<JanusGraphVertex> iterator = tx.query().vertices().iterator();
        while (iterator.hasNext()){
            JanusGraphVertex vertex = iterator.next();
            Iterator<VertexProperty<Object>> properties = vertex.properties();
            log.info(String.format("------------------------------------------vid->%s",vertex.id()));
           /* while(properties.hasNext()){
                VertexProperty<Object> property = properties.next();
                String key = property.key();
                Object value = property.value();
                log.info(String.format("key->%s,value->%s",key,value));
            }*/
        }
        Iterator<JanusGraphEdge> edges = tx.query().edges().iterator();
        while (edges.hasNext()){
            JanusGraphEdge edge = edges.next();
            JanusGraphVertex in = edge.inVertex();
            JanusGraphVertex out = edge.otherVertex(in);
            log.info(String.format("关系->%s,v->%s,v->%s",edge.id(),out.longId(),in.longId()));
        }
    }

    @Test
    /**
     * 产生uid=1,2,3的三个顶点，两条关系1->2,1->3
     */
    public void produceData(){
        int numV = 3;
        int numE = generateGraph();
        clopen();
    }

    @Test
    public void kgCounter() throws Exception {
        final JanusGraphComputer computer = graph.compute();
        KGMapper kgMapper = new KGMapper();
        kgMapper.loadState(graph,graph.configuration());
        computer.resultMode(JanusGraphComputer.ResultMode.NONE);
        computer.workers(1);
        //computer.program(new KGCounter());
        computer.mapReduce(kgMapper);
        ComputerResult result = computer.submit().get();
        System.out.println("Execution time (ms) " + result.memory().getRuntime());
        assertTrue(result.memory().exists(KGMapper.DEGREE_RESULT));
        Map<Long,Integer> degrees = result.memory().get(KGMapper.DEGREE_RESULT);
        int totalCount = 0;
        for (Map.Entry<Long,Integer> entry : degrees.entrySet()) {
            int degree = entry.getValue();
            final JanusGraphVertex v = getV(tx, entry.getKey());
            int count = v.value("uid");
            totalCount+= degree;
        }
        System.out.println(222);
    }


    private int generateRandomGraph(int numV) {
        mgmt.makePropertyKey("uid").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.MULTI).make();
        mgmt.makePropertyKey("values").cardinality(Cardinality.LIST).dataType(Integer.class).make();
        mgmt.makePropertyKey("numvals").dataType(Integer.class).make();
        finishSchema();
        int numE = 0;
        JanusGraphVertex[] vs = new JanusGraphVertex[numV];
        for (int i=0;i<numV;i++) {
            vs[i] = tx.addVertex("uid",i+1);
            int numberOfValues = random.nextInt(5)+1;
            vs[i].property(VertexProperty.Cardinality.single, "numvals", numberOfValues);
            for (int j=0;j<numberOfValues;j++) {
                vs[i].property("values",random.nextInt(100));
            }
        }
        for (int i=0;i<numV;i++) {
            int edges = i+1;
            JanusGraphVertex v = vs[i];
            for (int j=0;j<edges;j++) {
                JanusGraphVertex u = vs[random.nextInt(numV)];
                v.addEdge("knows", u);
                numE++;
            }
        }
        assertEquals(numV*(numV+1),numE*2);
        return numE;
    }


    private int generateGraph() {
        mgmt.makePropertyKey("uid").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.MULTI).make();
        mgmt.makePropertyKey("values").cardinality(Cardinality.LIST).dataType(Integer.class).make();
        mgmt.makePropertyKey("numvals").dataType(Integer.class).make();
        finishSchema();
        JanusGraphVertex v1 = tx.addVertex("uid", 1);
        int numberOfValues = random.nextInt(5)+1;
        v1.property(VertexProperty.Cardinality.single, "numvals", numberOfValues);
        for (int j=0;j<numberOfValues;j++) {
            v1.property("values",random.nextInt(100));
        }
        JanusGraphVertex v2 = tx.addVertex("uid", 2);
        numberOfValues = random.nextInt(5)+1;
        v2.property(VertexProperty.Cardinality.single, "numvals", numberOfValues);
        for (int j=0;j<numberOfValues;j++) {
            v2.property("values",random.nextInt(100));
        }
        JanusGraphVertex v3 = tx.addVertex( "uid", 3);
        numberOfValues = random.nextInt(5)+1;
        v3.property(VertexProperty.Cardinality.single, "numvals", numberOfValues);
        for (int j=0;j<numberOfValues;j++) {
            v3.property("values",random.nextInt(100));
        }
        v1.addEdge("knows", v2);
        v1.addEdge("knows", v3);
        log.info("v1->"+v1.id());
        log.info("v2->"+v2.id());
        log.info("v3->"+v3.id());
        return 2;
    }

    public static class KGCounter extends StaticVertexProgram<Integer> {

        public static final String DEGREE = "degree";
        public static final MessageCombiner<Integer> ADDITION = (a,b) -> a+b;
        public static final MessageScope.Local<Integer> DEG_MSG = MessageScope.Local.of(__::inE);

        private final int length;

        public KGCounter() {
            this(1);
        }

        public KGCounter(int length) {
            Preconditions.checkArgument(length>0);
            this.length = length;
        }

        @Override
        public void setup(Memory memory) {
        }

        @Override
        public void execute(Vertex vertex, Messenger<Integer> messenger, Memory memory) {
            if (memory.isInitialIteration()) {
                messenger.sendMessage(DEG_MSG, 1);
            } else {
                int degree = IteratorUtils.stream(messenger.receiveMessages()).reduce(0, (a, b) -> a + b);
                vertex.property(VertexProperty.Cardinality.single, DEGREE, degree);
                if (memory.getIteration()<length) {
                    messenger.sendMessage(DEG_MSG, degree);
                }
            }
        }

        @Override
        public boolean terminate(Memory memory) {
            return memory.getIteration()>=length;
        }

        @Override
        public Set<VertexComputeKey> getVertexComputeKeys() {
            return new HashSet<>(Collections.singletonList(VertexComputeKey.of(DEGREE, false)));
        }

        @Override
        public Set<MemoryComputeKey> getMemoryComputeKeys() {
            return new HashSet<>(Collections.singletonList(MemoryComputeKey.of(DEGREE, Operator.assign, true, false)));
        }

        @Override
        public Optional<MessageCombiner<Integer>> getMessageCombiner() {
            return Optional.of(ADDITION);
        }

        @Override
        public Set<MessageScope> getMessageScopes(Memory memory) {
            if (memory.getIteration()<length) return ImmutableSet.of(DEG_MSG);
            else return Collections.emptySet();
        }

        // TODO i'm not sure these preferences are correct

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.NEW;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.VERTEX_PROPERTIES;
        }

        @Override
        public VertexProgram.Features getFeatures() {
            return new VertexProgram.Features() {
                @Override
                public boolean requiresLocalMessageScopes() {
                    return true;
                }

                @Override
                public boolean requiresVertexPropertyAddition() {
                    return true;
                }
            };
        }


    }

    public static class KGMapper extends StaticMapReduce<Long,Integer,Long,Integer,Map<Long,Integer>> {

        public static final String DEGREE_RESULT = "degrees";
        private Graph graph;
        private Configuration configuration;

        public void loadState(Graph graph, Configuration configuration) {
            this.graph=graph;
            this.configuration=configuration;
        }

        @Override
        public boolean doStage(MapReduce.Stage stage) {
            return true;
        }

        @Override
        public void map(Vertex vertex, MapEmitter<Long, Integer> emitter) {
            Integer uid = vertex.value("uid");
            emitter.emit((Long)vertex.id(),uid);
            //emitter.emit((Long)vertex.id(),vertex.value(KGCounter.DEGREE));
            if(uid>2) {
                GraphTraversalSource g = graph.traversal();
                g.V(vertex.id()).drop().tryNext();
                g.tx().commit();
            }
        }

        @Override
        public void combine(Long key, Iterator<Integer> values, ReduceEmitter<Long, Integer> emitter) {
            this.reduce(key,values,emitter);
        }

        @Override
        public void reduce(Long key, Iterator<Integer> values, ReduceEmitter<Long, Integer> emitter) {
            int count = 0;
            while (values.hasNext()) {
                count = count + values.next();
            }
            emitter.emit(key, count);
            /*GraphTraversalSource g = graph.traversal();
            g.V(vertex.id()).drop().iterate();
            g.tx().commit();*/
        }

        @Override
        public Map<Long, Integer> generateFinalResult(Iterator<KeyValue<Long, Integer>> keyValues) {
            Map<Long,Integer> result = new HashMap<>();
            for (; keyValues.hasNext(); ) {
                KeyValue<Long, Integer> r =  keyValues.next();
                result.put(r.getKey(),r.getValue());
            }
            return result;
        }

        @Override
        public String getMemoryKey() {
            return DEGREE_RESULT;
        }

    }
}
