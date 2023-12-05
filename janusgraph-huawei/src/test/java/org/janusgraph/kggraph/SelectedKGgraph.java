package org.janusgraph.kggraph;

import com.google.common.base.Stopwatch;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.ser.AbstractGraphSONMessageSerializerV2d0;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.process.remote.traversal.step.map.RemoteStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MinGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.LazyBarrierStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.janusgraph.dsl.KydsjTraversal;
import org.janusgraph.dsl.KydsjTraversalSource;
import org.janusgraph.dsl.__;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.kggraph.json.TrsGraphSONMapper;
import org.janusgraph.kggraph.json.TrsGraphSONVersion;
import org.janusgraph.util.encoding.LongEncoding;
import org.janusgraph.util.system.DefaultKeywordField;
import org.janusgraph.util.system.DefaultTextField;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.script.Bindings;

/**
 * @author: ldp
 * @time: 2021/1/18 15:27
 * @jira:
 */
public class SelectedKGgraph extends AbstractKGgraphTest{
    private static final Logger LOGGER = LoggerFactory.getLogger(SelectedKGgraph.class);
    @Test
    public void script(){
        Stopwatch started = Stopwatch.createStarted();
        //String gremlin = "g.V().limit(10)";
       /* String gremlin = "nodes = g.V(\"000558e1d27c8b99_24_000\").as(\"node\").both().as(\"node\").select(\"node\").unfold().valueMap().with(WithOptions.tokens).fold().inject(__.V(\"000558e1d27c8b99_24_000\").valueMap().with(WithOptions.tokens)).unfold()\n" +
            "edges = g.V(\"000558e1d27c8b99_24_000\").bothE() \n [nodes.toList(),edges.toList()]";*/

        String gremlin = "nodes = g.V(\"000558e1d27c8b99_24_000\").both().elementMap()\n" +
            "edges = g.V(\"000558e1d27c8b99_24_000\").bothE() \n [nodes.toList(),edges.toList()]";

        //String gremlin = "g.V(\"000558e1d27c8b99_24_000\").bothE().otherV().path()";
        Bindings bindings = engine.createBindings();
        bindings.put("g", g);
        try{
            Object result = engine.eval(gremlin, bindings);
            if (result instanceof Traversal){
                Traversal.Admin graphTraversal = (Traversal.Admin<?,?>) result;
                //判断当前语句最后一步  是否是 RemoteStep 如果不是 就手动添加
                if(!(graphTraversal.getEndStep() instanceof RemoteStep)){
                    //这里底层调用  fill方法 返回类型
                    List<?> resultList = new ArrayList<>(10);
                    // fill 使用的 异常退出循环，数据量太多会出问题
                    graphTraversal.fill(resultList);
                    if (!resultList.isEmpty() && resultList.size() == 1){
                        result = resultList.get(0);
                    }else {
                        result = resultList;
                    }
                }
            }
            ResponseMessage responseMessage = convert(result, gremlin);
            GraphSONMapper.Builder version = GraphSONMapper.build().addRegistry(JanusGraphIoRegistry.instance()).version(GraphSONVersion.V3_0);
            GraphSONMessageSerializerV3d0 gson = new GraphSONMessageSerializerV3d0(version);
            String json = gson.serializeResponseAsString(responseMessage);
           // String trsJson = serializeResponseAsString(responseMessage);
            started.stop();
            LOGGER.info(String.format("gremlin script:%s,执行用时%s",gremlin,started.elapsed(TimeUnit.MILLISECONDS)));
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    public String serializeResponseAsString(final ResponseMessage responseMessage) throws SerializationException {
        try {
            ObjectMapper mapper=objectMapper();
            return mapper.writeValueAsString(responseMessage);
        } catch (Exception ex) {
            throw new SerializationException(ex);
        }
    }
    private ObjectMapper objectMapper(){
        TrsGraphSONMapper.Builder version = TrsGraphSONMapper.build().includePropertyProperty(false).addRegistry(JanusGraphIoRegistry.instance()).version(TrsGraphSONVersion.TRS);
        ObjectMapper mapper = configureBuilder(version).create().createMapper();
        return mapper;
    }

    private TrsGraphSONMapper.Builder configureBuilder(final TrsGraphSONMapper.Builder builder) {
        return builder.version(TrsGraphSONVersion.TRS).addCustomModule(new AbstractGraphSONMessageSerializerV2d0.GremlinServerModule());
    }

    private ResponseMessage convert(Object result, String script) {
        org.apache.tinkerpop.gremlin.driver.message.RequestMessage.Builder build = RequestMessage.build(script);
        ResponseMessage.Builder responseMessageBuilder = ResponseMessage.build(build.create());
        ResponseMessage responseMessage = responseMessageBuilder.result(result).create();
        return responseMessage;
    }
    @Test
    public void gremlinDsl() throws SerializationException {
        Stopwatch started = Stopwatch.createStarted();
       /* List<Map<Object, Object>> vertices = g.V("000c118ea9e9f45d_9_000").as("node").both()
            .as("node").unfold().elementMap("title").toList();*/
        List<Map<String, Object>> vertices = g.V("000558e1d27c8b99_24_000")
            .as("a").elementMap().as("a1")
            .select("a").bothE().as("b").elementMap().as("b1")
            .select("b").otherV().as("c").elementMap().as("c1")
            .select("a1", "b1", "c1").toList();
        //List<Vertex> vertices = g.V().both().barrier(1).both().limit(100).toList();
      /*  List<Map<Object, Object>> vertices = g.V("000558e1d27c8b99_24_000").as("a")
            .both().valueMap().as("b").toList();*/

        //List<Vertex> vertices = g.V().limit(10).toList();
        ResponseMessage responseMessage = convert(vertices, "g.V().limit(10).toList()");
        GraphSONMapper.Builder version = GraphSONMapper.build().addRegistry(JanusGraphIoRegistry.instance()).version(GraphSONVersion.V3_0);
        GraphSONMessageSerializerV3d0 gson = new GraphSONMessageSerializerV3d0(version);
        String json = gson.serializeResponseAsString(responseMessage);
        started.stop();
        LOGGER.info("verteices size:"+vertices.size()+",times:"+started.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    public void gremlinGroovlDsl(){
        Stopwatch started = Stopwatch.createStarted();
        List<Vertex> vertices = g.withoutStrategies(LazyBarrierStrategy.class).V().both().both().limit(100).toList();
        started.stop();
        LOGGER.info("verteices size:"+vertices.size()+",times:"+started.elapsed(TimeUnit.MILLISECONDS));
    }


    /**
     * 根据tid查询顶点对象
     */
    @Test
    public void selectByTid1(){
        String tid="tid002";
        Vertex next = g.T(tid).next();
        System.out.println(next.label());
        Iterator<VertexProperty<Object>> qq_num_properties = next.properties();
        while (qq_num_properties.hasNext()){
            VertexProperty<Object> vertexProperty = qq_num_properties.next();
            if(vertexProperty.isPresent()){
                Object value = vertexProperty.value();
                System.out.println(vertexProperty.key()+"->"+value);
                Iterator<Property<Object>> properties = vertexProperty.properties();
                while (properties.hasNext()){
                    Property<Object> property = properties.next();
                    if(property.isPresent()){
                        Object value1 = property.value();
                        System.out.println(property.key()+"<->"+value1);
                    }
                }
            }
        }
    }
    /**
     * 根据tid查询顶点对象
     */
    @Test
    public void selectByTid2(){
        String tid="tid0017";
        String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
        Vertex next = g.V(graphId).next();
        Iterator<VertexProperty<Object>> qq_num_properties = next.properties();
        while (qq_num_properties.hasNext()){
            VertexProperty<Object> vertexProperty = qq_num_properties.next();
            if(vertexProperty.isPresent()){
                Object value = vertexProperty.value();
                System.out.println(vertexProperty.key()+"->"+value);
                Iterator<Property<Object>> properties = vertexProperty.properties();
                while (properties.hasNext()){
                    Property<Object> property = properties.next();
                    if(property.isPresent()){
                        Object value1 = property.value();
                        System.out.println(property.key()+"<->"+value1);
                    }
                }
            }
        }
    }

    @Test
    public void selectByTidOther(){
        String tid="tid003";
        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.buildTransaction().consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start();
        KydsjTraversalSource g = tx.traversal(KydsjTraversalSource.class);
        Optional<Vertex> vertex = g.T(tid).tryNext();
        if(vertex.isPresent()) {
            Vertex next = vertex.get();
            Iterator<VertexProperty<Object>> qq_num_properties = next.properties();
            while (qq_num_properties.hasNext()) {
                VertexProperty<Object> vertexProperty = qq_num_properties.next();
                if (vertexProperty.isPresent()) {
                    Object value = vertexProperty.value();
                    System.out.println(vertexProperty.key() + "->" + value);
                    Iterator<Property<Object>> properties = vertexProperty.properties();
                    while (properties.hasNext()) {
                        Property<Object> property = properties.next();
                        if (property.isPresent()) {
                            Object value1 = property.value();
                            System.out.println(property.key() + "<->" + value1);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void selectLink(){
        Optional<Vertex> vertex = g.T("686").tryNext();
        System.out.println("开始.....");
        Stopwatch started = Stopwatch.createStarted();
        Long next = g.T("686", "354", "200", "202", "232", "890", "12", "233", "190", "122").out().count().next();
        Map<Object, Object> label = g.T("686", "354", "200", "202", "232", "890", "12", "233", "190", "122").out().group().by("age1").by(__.count()).next();
        Map<Object, Object> label2 = g.T("686", "354", "200", "202", "232", "890", "12", "233", "190", "122").out().group().by("age1").by(__.count()).next();
        started.stop();
        LOGGER.info("条数："+label.size()+"用时："+started.elapsed(TimeUnit.MILLISECONDS));
    }
    @Test
    public void count(){
        //Long next = g.V().hasLabel("object_qq").has(DefaultTextField.TITLE.getName(), "我是测试标签").count().next();
        Long next = g.V().hasLabel("object_qq").has(DefaultTextField.TITLE.getName(),"我是测试标签").count().next();
        LOGGER.info("数量："+next);
    }
    @Test
    public void selectBothLink(){
        Optional<Vertex> vertex = g.V().hasLabel("object_qq").has(DefaultTextField.TITLE.getName(), "我是测试标签").both().tryNext();
        if(vertex.isPresent()){
            Vertex next = vertex.get();
            Iterator<VertexProperty<Object>> qq_num_properties = next.properties();
            while (qq_num_properties.hasNext()){
                VertexProperty<Object> vertexProperty = qq_num_properties.next();
                if(vertexProperty.isPresent()){
                    Object value = vertexProperty.value();
                    System.out.println(vertexProperty.key()+"->"+value);
                    Iterator<Property<Object>> properties = vertexProperty.properties();
                    while (properties.hasNext()){
                        Property<Object> property = properties.next();
                        if(property.isPresent()){
                            Object value1 = property.value();
                            System.out.println(property.key()+"<->"+value1);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void selectLinkId(){
        Optional<Edge> optionalEdge = g.E("2560016_28-tid003_28_000-416010101-tid004_27_000").tryNext();
        if(optionalEdge.isPresent()){
            Edge edge = optionalEdge.get();
            Iterator<Property<Object>> edgeProperties = edge.properties();
            while (edgeProperties.hasNext()){
                Property<Object> edgeProperty = edgeProperties.next();
                if(edgeProperty.isPresent()){
                    Object value = edgeProperty.value();
                    System.out.println(edgeProperty.key()+"->"+value);
                }
            }
        }
    }

    @Test
    public void selectLinkId2(){
        Optional<Edge> optionalEdge = g.E("1280016_28-tid003_28_000-416010101-tid004_27_000").tryNext();
        if(optionalEdge.isPresent()){
            Edge edge = optionalEdge.get();
            Iterator<Property<Object>> edgeProperties = edge.properties();
            while (edgeProperties.hasNext()){
                Property<Object> edgeProperty = edgeProperties.next();
                if(edgeProperty.isPresent()){
                    Object value = edgeProperty.value();
                    System.out.println(edgeProperty.key()+"->"+value);
                }
            }
        }
    }



    @Test
    public void selectTid() {
        Optional<Vertex> tid003 = g.V().has(DefaultKeywordField.TID.getName(), "tid0017").tryNext();
        if(tid003.isPresent()){
            Vertex next = tid003.get();
            Iterator<VertexProperty<Object>> qq_num_properties = next.properties();
            while (qq_num_properties.hasNext()){
                VertexProperty<Object> vertexProperty = qq_num_properties.next();
                if(vertexProperty.isPresent()){
                    Object value = vertexProperty.value();
                    System.out.println(vertexProperty.key()+"->"+value);
                    Iterator<Property<Object>> properties = vertexProperty.properties();
                    while (properties.hasNext()){
                        Property<Object> property = properties.next();
                        if(property.isPresent()){
                            Object value1 = property.value();
                            System.out.println(property.key()+"<->"+value1);
                        }
                    }
                }
            }
        }
    }

    /**
     * mixed索引必须指定label才能使用索引
     */
    @Test
    public void selectObjLabel() {
        //Optional<Vertex> tid003 = g.V().hasLabel("object_qq").has(DefaultTextField.TITLE.getName(),"我是测试标签").tryNext();
        Optional<Vertex> tid003 = g.V().hasLabel("object_qq").has(DefaultTextField.TITLE.getName(),"我是测试不一样").tryNext();
       // Optional<Vertex> tid003 = g.V().hasLabel("object_qq").has("name","我是测试14qq").tryNext();
        if(tid003.isPresent()){
            Vertex next = tid003.get();
            Iterator<VertexProperty<Object>> qq_num_properties = next.properties();
            while (qq_num_properties.hasNext()){
                VertexProperty<Object> vertexProperty = qq_num_properties.next();
                if(vertexProperty.isPresent()){
                    Object value = vertexProperty.value();
                    System.out.println(vertexProperty.key()+"->"+value);
                    Iterator<Property<Object>> properties = vertexProperty.properties();
                    while (properties.hasNext()){
                        Property<Object> property = properties.next();
                        if(property.isPresent()){
                            Object value1 = property.value();
                            System.out.println(property.key()+"<->"+value1);
                        }
                    }
                }
            }
        }
    }

    /**
     * mixed索引必须指定label才能使用索引
     */
    @Test
    public void selectObj() {
        List<Vertex> vertices = g.V().has("qq_num", "张三").toList();
        System.out.println("条数-》》》》》》》》》》》》》》》》》》》》》》》"+vertices.size());
        for(Vertex next:vertices){
            Iterator<VertexProperty<Object>> qq_num_properties = next.properties();
            while (qq_num_properties.hasNext()){
                VertexProperty<Object> vertexProperty = qq_num_properties.next();
                if(vertexProperty.isPresent()){
                    Object value = vertexProperty.value();
                    System.out.println(vertexProperty.key()+"->"+value);
                    Iterator<Property<Object>> properties = vertexProperty.properties();
                    while (properties.hasNext()){
                        Property<Object> property = properties.next();
                        if(property.isPresent()){
                            Object value1 = property.value();
                            System.out.println(property.key()+"<->"+value1);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void readElements() {
        try {
            if (g == null) {
                return;
            }
            //Vertex next = g.V(LongEncoding.decode("qqqun$333_25_000")).next();
            /*Iterable<JanusGraphVertex> vertices = getJanusGraph().query().limit(10).vertices();
            vertices.forEach(janusGraphVertex -> {
                Iterable<JanusGraphEdge> edgesOut = janusGraphVertex.query().direction(Direction.OUT).edges();
                Iterable<JanusGraphEdge> edgesIn = janusGraphVertex.query().direction(Direction.IN).edges();
                System.out.println(Iterables.size(edgesOut));
                System.out.println(Iterables.size(edgesIn));
            });*/
            Vertex next = g.V("686").next();
            Iterator<VertexProperty<Object>> qq_num_properties = next.properties("name");
            while (qq_num_properties.hasNext()){
                VertexProperty<Object> vertexProperty = qq_num_properties.next();
                if(vertexProperty.isPresent()){
                    Object value = vertexProperty.value();
                    System.out.println(vertexProperty.key()+"->"+value);
                    Iterator<Property<Object>> properties = vertexProperty.properties();
                    while (properties.hasNext()){
                        Property<Object> property = properties.next();
                        if(property.isPresent()){
                            Object value1 = property.value();
                            System.out.println(property.key()+"<->"+value1);
                        }
                    }
                }
            }
            List<Map<Object, Object>> maps = g.V(LongEncoding.decode("qqqun$333_25_000")).elementMap().toList();
            List<Map<Object, Object>> out = g.V("qqqun$333_25_000").out().limit(2).in().elementMap().toList();
            List<Map<Object, Object>> in = g.V(LongEncoding.decode("990xbbkg123000")).in().elementMap().toList();
            List<Map<Object, Object>> others = g.V(LongEncoding.decode("990xbbkg123000")).both().elementMap().toList();
            List<Vertex> vertices4 = g.V().hasLabel("object_qqqun").has("qqqun_num", "308").limit(1).toList();

        } finally {
            g.tx().rollback();
        }
    }

    @Test
    public void test(){
        //KydsjTraversal<Vertex, ? extends Property<Object>> eidentify_fbrgzs1 = g.V("56bb138364cd94e2a87078d84bf013c5_22_000").both().properties("eidentify_fbrgzs");
        //List<? extends Property<Object>> eidentify_fbrgzs = g.V("56bb138364cd94e2a87078d84bf013c5_22_000").both().properties("eidentify_fbrgzs").toList();
        //List<Object> eidentify_fbrgzs2 = g.V("56bb138364cd94e2a87078d84bf013c5_22_000").both().properties("eidentify_fbrgzs").value().toList();
        //List<Object> eidentify_fbrgzs3 = g.V("56bb138364cd94e2a87078d84bf013c5_22_000").both().properties("eidentify_fbrgzs").values().toList();
        //KydsjTraversal<Vertex, Long> result = g.V("56bb138364cd94e2a87078d84bf013c5_22_000").both().count();
        //KydsjTraversal<Vertex, Comparable> result = g.V("56bb138364cd94e2a87078d84bf013c5_22_000").both().properties("eidentify_fbrgzs").value().max();
        KydsjTraversal<Vertex, Object> result = g.V("56bb138364cd94e2a87078d84bf013c5_22_000").both().properties("eidentify_fbrgzs").value();
        //这里底层调用  fill方法 返回类型
        List<?> resultList = new ArrayList<>();
        if (result instanceof Traversal){
            Traversal.Admin graphTraversal = (Traversal.Admin<?,?>) result;
            if(graphTraversal.getEndStep() instanceof PropertyValueStep){
                // fill 使用的 异常退出循环，数据量太多会出问题
                graphTraversal.fill(resultList);
            }else if(graphTraversal.getEndStep() instanceof MaxGlobalStep){
                // fill 使用的 异常退出循环，数据量太多会出问题
                graphTraversal.fill(resultList);
            }else if(graphTraversal.getEndStep() instanceof MinGlobalStep){
                // fill 使用的 异常退出循环，数据量太多会出问题
                graphTraversal.fill(resultList);
            }else if(graphTraversal.getEndStep() instanceof MeanGlobalStep){
                // fill 使用的 异常退出循环，数据量太多会出问题
                graphTraversal.fill(resultList);
            }else if(graphTraversal.getEndStep() instanceof SumGlobalStep){
                // fill 使用的 异常退出循环，数据量太多会出问题
                graphTraversal.fill(resultList);
            }else if(graphTraversal.getEndStep() instanceof CountGlobalStep){
                // fill 使用的 异常退出循环，数据量太多会出问题
                graphTraversal.fill(resultList);
            }
        }
        System.out.println(22);
    }
}
