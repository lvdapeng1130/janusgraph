package org.janusgraph.kydsj;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.*;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.kydsj.serialize.MediaData;
import org.janusgraph.kydsj.serialize.Note;
import org.janusgraph.util.encoding.LongEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author: ldp
 * @time: 2020/7/20 13:42
 * @jira:
 */
public class KyGraphApp1 extends JanusGraphApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(KyGraphApp1.class);
    public KyGraphApp1(String fileName) {
        super(fileName);
    }

    /**
     * Creates the vertex labels.
     */
    @Override
    protected void createVertexLabels(final JanusGraphManagement management) {
        management.makeVertexLabel("person").make();
        management.makeVertexLabel("doctor").make();
        management.makeVertexLabel("teacher").make();
        management.makeVertexLabel("phone").make();
        management.makeVertexLabel("location").make();
        management.makeVertexLabel("demigod").make();
        management.makeVertexLabel("human").make();
        management.makeVertexLabel("monster").make();
    }

    /**
     * Creates the edge labels.
     */
    @Override
    protected void createEdgeLabels(final JanusGraphManagement management) {
        management.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();
        management.makeEdgeLabel("mother").multiplicity(Multiplicity.MANY2ONE).make();
        management.makeEdgeLabel("lives").signature(management.getPropertyKey("reason")).make();
        management.makeEdgeLabel("pet").make();
        management.makeEdgeLabel("brother").make();
        management.makeEdgeLabel("battled").make();
    }

    /**
     * Creates the properties for vertices, edges, and meta-properties.
     */
    @Override
    protected void createProperties(final JanusGraphManagement management) {
        management.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("age").dataType(Integer.class).make();
        management.makePropertyKey("school_name").dataType(String.class).make();
        management.makePropertyKey("hospital_name").dataType(String.class).make();
        management.makePropertyKey("text").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("time").dataType(Integer.class).make();
        management.makePropertyKey("reason").dataType(String.class).make();
        management.makePropertyKey("place").dataType(Geoshape.class).make();

        //属性内置属性定义
        management.makePropertyKey("startDate").dataType(Date.class).make();
        management.makePropertyKey("endDate").dataType(Date.class).make();
        management.makePropertyKey("geo").dataType(Geoshape.class).make();
        management.makePropertyKey("dsr").dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("role").dataType(String.class).make();
    }

    /**
     * Creates the composite indexes. A composite index is best used for
     * exact match lookups.
     */
    @Override
    protected void createCompositeIndexes(final JanusGraphManagement management) {
        management.buildIndex("nameIndex", Vertex.class).addKey(management.getPropertyKey("name")).buildCompositeIndex();
    }

    /**
     * Creates the mixed indexes. A mixed index requires that an external
     * indexing backend is configured on the graph instance. A mixed index
     * is best for full text search, numerical range, and geospatial queries.
     */
    @Override
    protected void createMixedIndexes(final JanusGraphManagement management) {
        if (useMixedIndex) {
        /* management.buildIndex("person", Vertex.class)
                .addKey(management.getPropertyKey("name"))
                .buildMixedIndex(mixedIndexConfigName);*/
            management.buildIndex("person", Vertex.class)
                .addKey(management.getPropertyKey("age"))
                .addKey(management.getPropertyKey("time"))
                .addKey(management.getPropertyKey("text"))
                .addKey(management.getPropertyKey("place"))
                .addKey(management.getPropertyKey("name"))
                .indexOnly(management.getVertexLabel("person"))
                .buildMixedIndex(mixedIndexConfigName);
            management.buildIndex("doctor", Vertex.class)
                .addKey(management.getPropertyKey("age"))
                .addKey(management.getPropertyKey("time"))
                .addKey(management.getPropertyKey("place"))
                .addKey(management.getPropertyKey("name"))
                .addKey(management.getPropertyKey("hospital_name"))
                .indexOnly(management.getVertexLabel("doctor"))
                .buildMixedIndex(mixedIndexConfigName);
            management.buildIndex("teacher", Vertex.class)
                .addKey(management.getPropertyKey("age"))
                .addKey(management.getPropertyKey("time"))
                .addKey(management.getPropertyKey("place"))
                .addKey(management.getPropertyKey("name"))
                .addKey(management.getPropertyKey("school_name"))
                .indexOnly(management.getVertexLabel("teacher"))
                .buildMixedIndex(mixedIndexConfigName);

            management.buildIndex("eReasonPlace", Edge.class).addKey(management.getPropertyKey("reason"))
                .addKey(management.getPropertyKey("place")).buildMixedIndex(mixedIndexConfigName);
        }
    }

    /**
     * Adds the vertices, edges, and properties to the graph.
     */
    public void createElements() {
        try {
            // naive check if the graph was previously created
            /*if (g.V().has("name", "saturn").hasNext()) {
                if (supportsTransactions) {
                    g.tx().rollback();
                }
                return;
            }*/

            LOGGER.info("creating elements");
            // see GraphOfTheGodsFactory.java
            /*final Vertex saturn = g.addV("person")
                .property("name", "saturn",
                    "startDate",new Date(),
                    "endDate",new Date(),
                    "dsr","测试dsr写入","dsr","ceshi",
                    "geo",Geoshape.point(22.22,113.1122),
                    "role","测试role"
                ) .next();*/
            final Vertex zhangsan = g.addV("person")
                .property("name", "张三",
                    "startDate",new Date(),
                    "endDate",new Date(),
                    "dsr","测试dsr写入",
                    "geo",Geoshape.point(22.22,113.1122),
                    "role","测试role"
                ).property("age", 66)
                .next();
            final Vertex zhangguoquan = g.addV("person")
                .property("name", "张国权")
                .property("name", "小张")
                .property("name", "小小张")
                .property("text","我是正文内容")
                .property("age", 30)
                .property("time", 199011)
                .property("place", Geoshape.point(38.1f, 23.7f)).next();
            //添加张三是张国权的父亲。
            g.V(zhangsan).as("a").V(zhangguoquan).addE("father").to("a").next();

            final Vertex zhangxiaoli = g.addV("doctor")
                .property("name", "张晓丽")
                .property("age", 30)
                .property("time", 199011)
                .property("place", Geoshape.point(38.1f, 23.7f)).next();
            //添加张三是张晓丽的父亲。
            g.V(zhangsan).as("a").V(zhangxiaoli).addE("father").to("a").next();

            //同名同姓同年龄的老师张三
            final Vertex zhangsanTeacher = g.addV("teacher")
                .property("name", "张三",
                    "startDate",new Date(),
                    "endDate",new Date(),
                    "dsr","我是一个老师",
                    "geo",Geoshape.point(23.33,114.4444),
                    "role","测试role"
                ).property("age", 66)
                .property("time", 197011)
                .next();
            if (supportsTransactions) {
                g.tx().commit();
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            if (supportsTransactions) {
                g.tx().rollback();
            }
        }
        if (useMixedIndex) {
            try {
                // mixed indexes typically have a delayed refresh interval
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    public void createElementsMediaDataAndNote() {
        try {
            LOGGER.info("创建一个顶点并添加一个附件和注释");
            MediaData mediaData=new MediaData();
            mediaData.setAclId("我是附件的aclID");
            mediaData.setFilename("文件名");
            mediaData.setMediaTitle("附件标题");
            mediaData.setKey("我是附件的key");
            mediaData.setMediaData("我是附件的内容".getBytes());
            mediaData.setDsr(Sets.newHashSet("我是附件的一个dsr"));
            Note note=new Note();
            note.setId("我是注释的id");
            note.setNoteTitle("我是注释的标题");
            note.setNoteData("我是注释的内容");
            note.setDsr(Sets.newHashSet("我是注释的dsr"));

            //为顶点添加一个附件和注释
            final Vertex mediaAndNote=g.addV("person")
                .property("name", "测试附件和注释")
                .property(BaseKey.VertexAttachment.name(),mediaData)
                .property(BaseKey.VertexNote.name(),note)
                .next();

            if (supportsTransactions) {
                g.tx().commit();
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            if (supportsTransactions) {
                g.tx().rollback();
            }
        }
        if (useMixedIndex) {
            try {
                // mixed indexes typically have a delayed refresh interval
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    public void appendOtherDsr() {
        try {
            // naive check if the graph was previously created
            LOGGER.info("追加图库dsr");
            g.V().hasLabel("person")
                .has("age", P.eq(66)).has("name","张三")
                .property("name", "张三",
                    "startDate",new Date(),
                    "endDate",new Date(),
                    "dsr","我是徐小侠",
                    "geo",Geoshape.point(22.22,113.1122),
                    "role","更改后的role"
                ).property("age", 10000).next();

            if (supportsTransactions) {
                g.tx().commit();
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            if (supportsTransactions) {
                g.tx().rollback();
            }
        }
        if (useMixedIndex) {
            try {
                // mixed indexes typically have a delayed refresh interval
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    public void appendOtherMediaData() {
        try {
            LOGGER.info("给图库顶点添加附件");
            MediaData mediaData=new MediaData();
            mediaData.setAclId("我是附件ewwew的aclID");
            mediaData.setFilename("文件eweew名");
            mediaData.setMediaTitle("附件标题4444444444444444444444444444");
            mediaData.setKey("列一份附件的key332222222222222");
            mediaData.setMediaData("我是附件的二位翁内容呃呃呃223323232".getBytes());
            mediaData.setDsr(Sets.newHashSet("我是附件的一个dsr"));
            g.V().hasLabel("person").has("name", Text.textContains("测试附件和注释"))
                .property(BaseKey.VertexAttachment.name(),mediaData).next();

            if (supportsTransactions) {
                g.tx().commit();
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            if (supportsTransactions) {
                g.tx().rollback();
            }
        }
        if (useMixedIndex) {
            try {
                // mixed indexes typically have a delayed refresh interval
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    @Override
    public void createSchema() {
        final JanusGraphManagement management = getJanusGraph().openManagement();
        try {
            // ;
            if (management.getRelationTypes(RelationType.class).iterator().hasNext()) {
                management.rollback();
                return;
            }
            LOGGER.info("creating schema");
            createProperties(management);
            createVertexLabels(management);
            createEdgeLabels(management);
            createCompositeIndexes(management);
            createMixedIndexes(management);
            management.commit();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
            management.rollback();
        }
    }

    public void indexQuery(){
        Stream<JanusGraphIndexQuery.Result<JanusGraphVertex>> resultStream = getJanusGraph().indexQuery("person", "v.age:5000").vertexStream();
        resultStream.forEach(r->{
            JanusGraphVertex element = r.getElement();
            System.out.println(element);
        });
    }
    /**
     * Runs some traversal queries to get data from the graph.
     */
    public void readElements() {
        try {
            if (g == null) {
                return;
            }
            LOGGER.info("reading elements");
            Vertex next = g.V(LongEncoding.decode("3b4")).next();
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
            // look up vertex by name can use a composite index in JanusGraph
            //final List<Map<Object, Object>> v = g.V().hasLabel("person","teacher").has("name","张三").has("age", P.eq(66)).valueMap(true).next(2);
            final List<Map<Object, Object>> v1= g.V().or(
                __.hasLabel("person")
                    .has("name","张三")
                    .has("age", P.eq(66)),
                __.hasLabel("teacher")
                    .has("name","张三")
                    .has("age", P.eq(66))).has("time",197011)
                .valueMap(true).next(2);
            // numerical range query can use a mixed index in JanusGraph
            final List<Object> list = g.V().hasLabel("person","teacher").has("name","张三").has("age", P.eq(66)).values("age").toList();
            LOGGER.info(list.toString());


        } finally {
            // the default behavior automatically starts a transaction for
            // any graph interaction, so it is best to finish the transaction
            // even for read-only graph query operations
            if (supportsTransactions) {
                g.tx().rollback();
            }
        }
    }

    public void hideVertex(){
        g.V().hasLabel("teacher")
            .has("name", "张三")
            .has("age", P.eq(66)).has("time", 197011).property(BaseKey.VertexExists.name(),false).next();
        if (supportsTransactions) {
            g.tx().commit();
        }
       /* List<Map<Object, Object>> maps = g.V().hasLabel("teacher")
            .has("name", "张三")
            .has("age", P.eq(66)).has("time", 197011).elementMap().toList();
        System.out.println(maps);*/
    }

    public void readMediaDataAndNotes() {
        try {
            if (g == null) {
                return;
            }
            LOGGER.info("reading elements");
            // look up vertex by name can use a composite index in JanusGraph
            //final List<Map<Object, Object>> v = g.V().hasLabel("person","teacher").has("name","张三").has("age", P.eq(66)).valueMap(true).next(2);
            Vertex next = g.V().hasLabel("person").has("name",
                Text.textContains("测试附件和注释")).next();
            long vertexId =Long.parseLong(next.id().toString());
            List<MediaData> mediaDatas = this.getJanusGraph().getMediaDatas(vertexId);
            List<Note> notes = this.getJanusGraph().getNotes(vertexId);
            // numerical range query can use a mixed index in JanusGraph
            LOGGER.info(String.format("当前顶点id->%s",vertexId));
            LOGGER.info("读取到的附件------------------------------------");
            if(mediaDatas!=null) {
                for (MediaData mediaData : mediaDatas) {
                    LOGGER.info(mediaData.toString());
                    LOGGER.info("--------------------------------------------");
                }
            }
            LOGGER.info("读取到的注释------------------------------------");
            if(notes!=null) {
                for (Note note : notes) {
                    LOGGER.info(note.toString());
                    LOGGER.info("--------------------------------------------");
                }
            }



        } finally {
            // the default behavior automatically starts a transaction for
            // any graph interaction, so it is best to finish the transaction
            // even for read-only graph query operations
            if (supportsTransactions) {
                g.tx().rollback();
            }
        }
    }


    /**
     * Run the entire application:
     * 1. Open and initialize the graph
     * 2. Define the schema
     * 3. Build the graph
     * 4. Run traversal queries to get data from the graph
     * 5. Make updates to the graph
     * 6. Close the graph
     */
    public void runApp() {
        try {
            // open and initialize the graph
            openGraph();
            // define the schema before loading data
           /* if (supportsSchema) {
                createSchema();
            }*/

            // build the graph structure
            //createElements();
            //createElementsMediaDataAndNote();
            //appendOtherDsr();
            //appendOtherMediaData();
            // read to see they were made
            //hideVertex();
            readElements();
            //readMediaDataAndNotes();
            //indexQuery();

           /* for (int i = 0; i < 3; i++) {
                try {
                    Thread.sleep((long) (Math.random() * 500) + 500);
                } catch (InterruptedException e) {
                    LOGGER.error(e.getMessage(), e);
                }
                // update some graph elements with changes
                updateElements();
                // read to see the changes were made
                readElements();
            }

            // delete some graph elements
            deleteElements();
            // read to see the changes were made
            readElements();*/

            // close the graph
            closeGraph();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }


    public static void main(String[] args) throws Exception {
        final String fileName = (args != null && args.length > 0) ? args[0] : null;
        final boolean drop = (args != null && args.length > 1) && "drop".equalsIgnoreCase(args[1]);
        final KyGraphApp1 app = new KyGraphApp1(fileName);
        if (drop) {
            app.openGraph();
            app.dropGraph();
        } else {
            app.runApp();
        }
    }

}
