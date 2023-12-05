package org.janusgraph.kggraph;

import com.google.common.base.Stopwatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.ser.AbstractGraphSONMessageSerializerV2d0;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.process.remote.traversal.step.map.RemoteStep;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
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
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.dsl.KydsjTraversal;
import org.janusgraph.dsl.KydsjTraversalSource;
import org.janusgraph.dsl.__;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.idhandling.IDHandler;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.kggraph.json.TrsGraphSONMapper;
import org.janusgraph.kggraph.json.TrsGraphSONVersion;
import org.janusgraph.qq.DefaultPropertyKey;
import org.janusgraph.util.encoding.LongEncoding;
import org.janusgraph.util.system.DefaultFields;
import org.janusgraph.util.system.DefaultKeywordField;
import org.janusgraph.util.system.DefaultTextField;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import static org.janusgraph.core.attribute.Text.textContains;
import static org.janusgraph.dsl.__.count;
import static org.janusgraph.dsl.__.group;
import static org.janusgraph.dsl.__.inE;
import static org.janusgraph.dsl.__.outE;
import static org.janusgraph.graphdb.database.idhandling.IDHandler.getBounds;

/**
 * @author: ldp
 * @time: 2021/1/18 15:27
 * @jira:
 */
@Slf4j
public class SelectedKGgraph extends AbstractKGgraphTest{
    private static final Logger LOGGER = LoggerFactory.getLogger(SelectedKGgraph.class);
    @Test
    public void xs(){
        Stopwatch stopwatch=Stopwatch.createStarted();
        List<Path> list = g.V("9eb75718876a97cd5d8891e6d73dc97d_18_000")
            .or(
                __.bothE().otherV().hasLabel("entity_wyh"),
                __.bothE().otherV().hasLabel("entity_wyh").union(__.bothE().otherV().has("~id", P.neq("9eb75718876a97cd5d8891e6d73dc97d_18_000"))),
                outE().otherV().hasLabel("entity_wyh"))
            .union(
                __.bothE().otherV().hasLabel("entity_wyh"),
                __.bothE().otherV().hasLabel("entity_wyh").union(__.bothE().otherV().has("~id", P.neq("9eb75718876a97cd5d8891e6d73dc97d_18_000"))),
                __.outE().otherV().hasLabel("entity_wyh")).limit(10000).path().toList();
        stopwatch.stop();
        System.out.println("用时："+stopwatch.elapsed(TimeUnit.MILLISECONDS)+",条数："+list.size());
        for(Object path:list){
           // System.out.println(path);
        }
    }

    /**
     * 两点最短路径，基于OLTP，单源
     */
    @Test
    public void dd(){
        List<Vertex> list = g.V("89dfef8085634335_23_000")
            .repeat(
                __.both().simplePath()
                    .sideEffect(__.hasId("befe67ed2309eb0e_2_000").aggregate("target"))
            )
            .emit(__.hasId("befe67ed2309eb0e_2_000"))
            .until(
                __.cap("target").unfold().dedup().hasId("befe67ed2309eb0e_2_000")
                    .or().loops().is(8)
            ).hasId("befe67ed2309eb0e_2_000").toList();
        for(Object path:list){
            System.out.println(path);
        }
    }
    @Test
    public void ss2(){
        List<Vertex> list =  g.withoutStrategies(LazyBarrierStrategy.class).V("89dfef8085634335_23_000").repeat(__.both().simplePath()
                .sideEffect(__.hasId("befe67ed2309eb0e_2_000").aggregate("target"))).
            until( __.cap("target").unfold().dedup().hasId("befe67ed2309eb0e_2_000")).hasId("befe67ed2309eb0e_2_000")
            .toList();
        for(Object path:list){
            System.out.println(path);
        }
    }

    @Test
    public void ss1(){
        List<Path> list = g.V().hasLabel("organization_qydw").has("organization_gsmc", "天津南大通用数据技术股份有限公司")
            .repeat("a",__.inE("link_gs").outV().has("bl", P.gt(25))
                .inE("link_hold").outV())
            .times(5)
            .emit()
            .simplePath()
            .path()
            .toList();
        for(Object path:list){
            System.out.println(path);
        }
    }

    @Test
    public void testEquelSliceQuery(){
        StaticBuffer[] bound = IDHandler.getBounds(RelationCategory.PROPERTY,false);
        SliceQuery sliceQuery = new SliceQuery(bound[0], bound[1]);
        StaticBuffer[] bound1 = IDHandler.getBounds(RelationCategory.PROPERTY,false);
        SliceQuery sliceQuery1 = new SliceQuery(bound1[0], bound1[1]);
        System.out.println(sliceQuery.equals(sliceQuery1));
    }

    @Test
    public void selectFull(){
        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.buildTransaction().consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start();
        KydsjTraversalSource g = tx.traversal(KydsjTraversalSource.class);
        Stopwatch started = Stopwatch.createStarted();
        Optional<Vertex> vertex = g.V().tryNext();
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
        started.stop();
        log.info("用时："+started.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    public void selectByFull(){
        String id="tid001_30_000";
        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.buildTransaction().consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start();
        KydsjTraversalSource g = tx.traversal(KydsjTraversalSource.class);
        Stopwatch started = Stopwatch.createStarted();
        Optional<Vertex> vertex = g.V(id).tryNext();
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
        started.stop();
        log.info("用时："+started.elapsed(TimeUnit.MILLISECONDS));
    }
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
    @Test
    public void gremlin(){
        Stopwatch started = Stopwatch.createStarted();
        Map<Object, Object> next = g.V("7950b0a93d23d71b88358b76beb36735_27_000")
            .bothE().limit(200000).group().by(T.label).by(__.count()).next();
        for(Map.Entry<Object, Object> entry:next.entrySet()){
            System.out.println(entry.getKey()+":"+entry.getValue());
        }
        started.stop();
        LOGGER.info("verteices size:"+next.size()+",times:"+started.elapsed(TimeUnit.MILLISECONDS));
    }
    @Test
    public void gremlin1(){
        Stopwatch started = Stopwatch.createStarted();
        Optional<Vertex> next = g.V().hasLabel("person").has("name", textContains("張昌財")).tryNext();
        started.stop();
        LOGGER.info("verteices size:"+next+",times:"+started.elapsed(TimeUnit.MILLISECONDS));
    }
    @Test
    public void gremlin3(){
        Stopwatch started = Stopwatch.createStarted();
        List<Edge> tss = g.V("qq2_18_000").bothE().toList();
        started.stop();
        LOGGER.info("verteices size:"+tss.size()+",times:"+started.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    public void gremlin5(){
        Stopwatch started = Stopwatch.createStarted();
        String tid="tid002";
        String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
        Optional<Vertex> qq_num = g.V(graphId).hasNot("qq_num1").tryNext();
        started.stop();
        LOGGER.info("verteices size:"+qq_num.isPresent()+",times:"+started.elapsed(TimeUnit.MILLISECONDS));
    }
    @Test
    public void gremlin6(){
        Stopwatch started = Stopwatch.createStarted();
        final JanusGraphManagement management = getJanusGraph().openManagement();
        try {
            PropertyKey propertyKey = management.getPropertyKey("id");
            management.rollback();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
            management.rollback();
        }
        List<Vertex> ids = g.V("000b236f3dc113cd_27_000").union(__.bothE().otherV().simplePath().has("~id", P.eq("52f0f797b6902a30_19_000"))).limit(5).toList();
        started.stop();
        LOGGER.info("verteices size:"+ids.size()+",times:"+started.elapsed(TimeUnit.MILLISECONDS));
    }
    @Test
    public void gremlin4() throws IOException {
        Stopwatch started = Stopwatch.createStarted();
        String tid="tid1002";
        String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
        String context= FileUtils.readFileToString(new File("D:\\xss\\activityList_5W.txt"), Charset.forName("UTF-8"));
        List<? extends Property<Object>> docs = g.V(graphId).properties(DefaultFields.DOCTEXT.getName()).toList();
        if(docs.size()>0){
            for(Property p:docs){
                if(p.isPresent()){
                    Object value = p.value();
                    System.out.println(context.length());
                    System.out.println(value.toString().length());
                }
            }
        }
        started.stop();
        LOGGER.info("verteices size:"+docs.size()+",times:"+started.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    public void aggregationLinkType(){
        Stopwatch started = Stopwatch.createStarted();
        List<Map<Object, Object>> list = g.V("9eb75718876a97cd5d8891e6d73dc97d_18_000").limit(10000).group().by(T.label).by(count()).toList();
        for(Map<Object,Object> maps:list) {
            for (Map.Entry<Object, Object> entry : maps.entrySet()) {
                System.out.println(entry.getKey() + ":" + entry.getValue());
            }
        }
        started.stop();
        LOGGER.info("verteices size:"+list.size()+",times:"+started.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    public void aggregationLinkTypeGroupByLinkText(){
        Stopwatch started = Stopwatch.createStarted();
        List<Map<Object, Object>> list = g.V("9eb75718876a97cd5d8891e6d73dc97d_18_000").bothE()
            .limit(1).group().by(T.label).by(group().by("link_text").by(count())).toList();
        for(Map<Object,Object> maps:list) {
            for (Map.Entry<Object, Object> entry : maps.entrySet()) {
                System.out.println(entry.getKey() + ":" + entry.getValue());
            }
        }
        started.stop();
        LOGGER.info("verteices size:"+list.size()+",times:"+started.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    public void ss() throws ScriptException {
        String traversalString = "g.V(\"9eb75718876a97cd5d8891e6d73dc97d_18_000\").bothE()";
        ScriptEngine engine = new GremlinGroovyScriptEngine();
        Bindings bindings = engine.createBindings();
        bindings.put("g", __.start());
        __.hasLabel("person").has("person_jtzz","aaaa");
        Traversal<?, ?> traversal = (Traversal<?, ?>) engine.eval(traversalString, bindings);
        System.out.println(222);
    }


    @Test
    public void aggregationLinkTypeGroupByLinkText1(){
        Stopwatch started = Stopwatch.createStarted();
        List<Map<Object, Object>> list = g.V("9eb75718876a97cd5d8891e6d73dc97d_18_000").bothE()
            .limit(100000).group().by("left_type").by(group().by(T.label).by(group().by("link_text").by(count()))).toList();
        for(Map<Object,Object> maps:list) {
            for (Map.Entry<Object, Object> entry : maps.entrySet()) {
                System.out.println(entry.getKey() + ":" + entry.getValue());
            }
        }
        started.stop();
        LOGGER.info("verteices size:"+list.size()+",times:"+started.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    public void aggregationLinkTypeGroupByLinkText2(){
        Stopwatch started = Stopwatch.createStarted();
        List<Map<Object, Object>> responseList;
        KydsjTraversal<Vertex, Vertex> kydsjTraversal = g.V("9eb75718876a97cd5d8891e6d73dc97d_18_000");
        KydsjTraversal<Vertex, Edge> traversalRight = outE();
        traversalRight=traversalRight.has("right_type", P.within("entity_wyh"));
        KydsjTraversal<Vertex, Map<Object, Object>> rightTraversal = traversalRight.limit(1000)
            .group().by(T.label).by(group().by("right_type").by(count()));
        KydsjTraversal<Vertex, Edge> traversalLeft = inE();
        traversalLeft=traversalLeft.has("left_type", P.within("entity_wyh"));
        KydsjTraversal<Vertex, Map<Object, Object>> leftTraversal = traversalLeft.limit(1000)
            .group().by(T.label).by(group().by("left_type").by(count()));
        responseList = kydsjTraversal.union(rightTraversal,leftTraversal).toList();
        if(responseList!=null){
            for(Map<Object, Object> objectObjects:responseList){
                for(Map.Entry<Object,Object> entry:objectObjects.entrySet()){
                    Object lType = entry.getKey();
                    if(lType!=null) {
                        String linkType = entry.getKey().toString();
                        Object value = entry.getValue();
                        if (value instanceof Map) {
                            Map<Object, Object> itemMap = (Map) value;
                            for (Map.Entry<Object, Object> entry1 : itemMap.entrySet()) {
                                Object oType = entry1.getKey();
                                if (oType != null) {
                                    String objectType = oType.toString();
                                    long counter = Long.parseLong(entry1.getValue().toString());
                                    System.out.println(String.format("%s,%s,%s",linkType, objectType, counter));
                                }else{

                                }
                            }
                        }
                    }
                }
            }
        }
        started.stop();
        LOGGER.info("verteices size:"+responseList.size()+",times:"+started.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    public void aggregationObjectType(){
        Stopwatch started = Stopwatch.createStarted();
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) graph.buildTransaction().consistencyChecks(false)
            .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
            try {
                KydsjTraversalSource g = threadedTx.traversal(KydsjTraversalSource.class);
                List<Map<Object, Object>> list = g.V("9eb75718876a97cd5d8891e6d73dc97d_18_000")
                    .union(outE().limit(10000).group().by("right_type").by(count()),inE().limit(10000).group().by("left_type").by(count())).toList();
                for(Map<Object,Object> maps:list) {
                    for (Map.Entry<Object, Object> entry : maps.entrySet()) {
                        System.out.println(entry.getKey() + ":" + entry.getValue());
                    }
                }
                started.stop();
                LOGGER.info("verteices size:"+list.size()+",times:"+started.elapsed(TimeUnit.MILLISECONDS));
            } finally {
                threadedTx.rollback();
            }
        }
    }
    @Test
    public void aggregationObjectType1(){
        Stopwatch started = Stopwatch.createStarted();
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) graph.buildTransaction().consistencyChecks(false)
            .checkInternalVertexExistence(false).checkExternalVertexExistence(false).start()) {
            try {
                KydsjTraversalSource g = threadedTx.traversal(KydsjTraversalSource.class);
                KydsjTraversal<Vertex, Edge> traversalRight = outE();
                KydsjTraversal<Vertex, Map<Object, Object>> rightTraversal = traversalRight.limit(10000)
                    .group().by("right_type").by(count());
                KydsjTraversal<Vertex, Edge> traversalLeft = inE();
                KydsjTraversal<Vertex, Map<Object, Object>> leftTraversal = traversalLeft.limit(10000)
                    .group().by("left_type").by(count());
                List<Map<Object, Object>> list = g.V("9eb75718876a97cd5d8891e6d73dc97d_18_000").union(rightTraversal,leftTraversal).toList();
                for(Map<Object,Object> maps:list) {
                    for (Map.Entry<Object, Object> entry : maps.entrySet()) {
                        System.out.println(entry.getKey() + ":" + entry.getValue());
                    }
                }
                started.stop();
                LOGGER.info("verteices size:"+list.size()+",times:"+started.elapsed(TimeUnit.MILLISECONDS));
            } finally {
                threadedTx.rollback();
            }
        }
    }
    @Test
    public void aggregation(){
        Stopwatch started = Stopwatch.createStarted();
        List<Map<Object, Object>> list = g.V("9eb75718876a97cd5d8891e6d73dc97d_18_000")
            .union(outE().limit(10000).group().by(T.label).by(group().by("right_type").by(count())),inE().limit(10000)
                .group().by(T.label).by(group().by("left_type").by(count()))).toList();
        for(Map<Object,Object> maps:list) {
            for (Map.Entry<Object, Object> entry : maps.entrySet()) {
                System.out.println(entry.getKey() + ":" + entry.getValue());
            }
        }
        started.stop();
        LOGGER.info("verteices size:"+list.size()+",times:"+started.elapsed(TimeUnit.MILLISECONDS));
    }

    /**
     * 根据tid查询顶点对象
     */
    @Test
    public void selectByEdge(){
        Edge next = g.E("link1_24-qq1_17_000-512010101-qqun1_24_000").next();
        System.out.println(next.label());
        Iterator<Property<Object>> qq_num_properties = next.properties();
        while (qq_num_properties.hasNext()){
            Property<Object> vertexProperty = qq_num_properties.next();
            if(vertexProperty.isPresent()){
                Object value = vertexProperty.value();
                System.out.println(vertexProperty.key()+"->"+value);
            }
        }
    }

    /**
     * 根据tid查询顶点对象
     */
    @Test
    public void selectByTid1(){
        String tid="0c3f697c3d1005c0";
        Vertex next = g.T(tid).next();
        System.out.println(next.label());
        Iterator<VertexProperty<Object>> qq_num_properties = next.properties();
        while (qq_num_properties.hasNext()){
            VertexProperty<Object> vertexProperty = qq_num_properties.next();
            if(vertexProperty.isPresent()){
                Object value = vertexProperty.value();
                System.out.println(vertexProperty.key()+"->"+value);
                Iterator<Property<Object>> dsrs = vertexProperty.properties(DefaultPropertyKey.DSR.getKey());
                while (dsrs.hasNext()){
                    Property<Object> property = dsrs.next();
                    if(property.isPresent()){
                        Object value1 = property.value();
                        System.out.println(property.key()+"<->"+value1);
                    }
                }
                System.out.println("--------------属性的属性(开始)-------------------");
                Iterator<Property<Object>> properties = vertexProperty.properties();
                while (properties.hasNext()){
                    Property<Object> property = properties.next();
                    if(property.isPresent()){
                        Object value1 = property.value();
                        System.out.println(property.key()+"<->"+value1);
                    }
                }
                System.out.println("--------------属性的属性(结束)-------------------");
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
    public void count1(){
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
