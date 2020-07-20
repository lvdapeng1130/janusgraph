package org.janusgraph.kydsj;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

            /*// define the schema before loading data
            if (supportsSchema) {
                createSchema();
            }

            // build the graph structure
            createElements();
            // read to see they were made
            readElements();

            for (int i = 0; i < 3; i++) {
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
