package org.janusgraph.kydsj;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author: ldp
 * @time: 2020/9/23 10:28
 * @jira:
 */
public class SelectTest extends JanusGraphApp{

    private static final Logger LOGGER = LoggerFactory.getLogger(SelectTest.class);

    public SelectTest(String fileName) {
        super(fileName);
    }

    private void printSchema(){
        final JanusGraphManagement management = getJanusGraph().openManagement();
        String printSchemaStr = management.printSchema();
        LOGGER.info(printSchemaStr);
        management.rollback();
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
            //List<Vertex> vertices = g.V().hasLabel("object_qq").has("name", Text.textContains("kizdnMZOO4aDwDFC2Y6XS5UaLnVCCT")).toList();
            List<Map<Object, Object>> maps = g.V().hasLabel("object_qqqun")
                .has("qqqun_num", Text.textContains("2250")).elementMap().toList();
            final List<Map<Object, Object>> maps1 = g.V().limit(10).elementMap().toList();
            List<Vertex> vertices = g.V().hasLabel("object_qqqun").has("qqqun_num", "2250").toList();
            //List<Vertex> vertices1 = g.V().hasLabel("object_qqqun").toList();
            System.out.println(vertices);
            //Vertex crc = g.V(LongEncoding.decode("9k0")).next();
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
            printSchema();
            readElements();
            //indexQuery();
            // close the graph
            closeGraph();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }


    public static void main(String[] args) throws Exception {
        final String fileName = (args != null && args.length > 0) ? args[0] : null;
        final boolean drop = (args != null && args.length > 1) && "drop".equalsIgnoreCase(args[1]);
        final SelectTest app = new SelectTest(fileName);
        if (drop) {
            app.openGraph();
            app.dropGraph();
        } else {
            app.runApp();
        }
    }
}
