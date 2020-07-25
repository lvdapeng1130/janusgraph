package org.janusgraph.kydsj;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author: ldp
 * @time: 2020/7/20 13:42
 * @jira:
 */
public class KyGraphApp extends JanusGraphApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(KyGraphApp.class);
    public KyGraphApp(String fileName) {
        super(fileName);
    }

    /**
     * Creates the vertex labels.
     */
    @Override
    protected void createVertexLabels(final JanusGraphManagement management) {
        management.makeVertexLabel("person").make();
        management.makeVertexLabel("location").make();
        management.makeVertexLabel("phone").make();
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
                .addKey(management.getPropertyKey("place"))
                .addKey(management.getPropertyKey("name"))
                .indexOnly(management.getVertexLabel("person"))
                .buildMixedIndex(mixedIndexConfigName);
            management.buildIndex("phone", Vertex.class)
                .addKey(management.getPropertyKey("age"))
                .indexOnly(management.getVertexLabel("phone"))
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
            final Vertex saturn = g.addV("person")
                .property("name", "saturn",
                    "startDate",new Date(),
                    "endDate",new Date(),
                    "dsr","测试dsr写入","dsr","测试dsr2",
                    "geo",Geoshape.point(22.22,113.1122),
                    "role","测试role"
                )
                .property("age", 10000).next();
            final Vertex phone = g.addV("phone")
                .property("name", "saturn")
                .property("age", 5000).next();
            final Vertex jupiter = g.addV("person")
                .property("name", "jupiter")
                .property("name", "jupiter1")
                .property("name", "jupiter2")
                .property("age", 5000,"time",22)
                .property("place", Geoshape.point(38.1f, 23.7f)).next();
            g.V(jupiter).as("a").V(saturn).addE("father").from("a").next();
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
            // see GraphOfTheGodsFactory.java
            g.V().hasLabel("person").has("age", P.eq(10000))
                .property("name", "saturn",
                    "startDate",new Date(),
                    "endDate",new Date(),
                    "dsr","我是徐小侠",
                    "geo",Geoshape.point(22.22,113.1122),
                    "role","测试rol1111e"
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
            // look up vertex by name can use a composite index in JanusGraph
            final List<Map<Object, Object>> v = g.V().hasLabel("person","phone").has("age", P.eq(5000)).valueMap(true).next(2);
            // numerical range query can use a mixed index in JanusGraph
            final List<Object> list = g.V().hasLabel("person","phone").has("age", P.eq(5000)).values("age").toList();
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
           /*if (supportsSchema) {
                createSchema();
            }

            // build the graph structure*/
            //createElements();
            appendOtherDsr();
            // read to see they were made
            //readElements();
            //indexQuery();

            /*for (int i = 0; i < 3; i++) {
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
        final KyGraphApp app = new KyGraphApp(fileName);
        if (drop) {
            app.openGraph();
            app.dropGraph();
        } else {
            app.runApp();
        }
    }

}
