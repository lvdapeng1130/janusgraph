package org.janusgraph.graphdb.database.leader;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author: ldp
 * @time: 2020/8/17 20:39
 * @jira:
 */
public class LeaderLatchAdapter implements Closeable {
    private static final Logger log =
        LoggerFactory.getLogger(LeaderLatchAdapter.class);
    public static final String INSTANCENODE="instance";
    public static final String LEADERNODE="/leader";
    private final String name;
    private final LeaderLatch leaderLatch;
    private final StandardJanusGraph janusGraph;
    private final CuratorFramework client;

    public LeaderLatchAdapter(CuratorFramework client, String name, StandardJanusGraph janusGraph) {
        this.client=client;
        this.name = name;
        this.janusGraph = janusGraph;
        this.leaderLatch = new LeaderLatch(client, LEADERNODE, name);
        this.leaderLatch.addListener(new JanusgraphLeaderLatchListener(janusGraph,client));
    }

    public void start() throws Exception {
        leaderLatch.start();
    }

    public LeaderLatch getLeaderLatch() {
        return leaderLatch;
    }

    @Override
    public void close() throws IOException {
        leaderLatch.close();
    }
    class JanusgraphLeaderLatchListener implements  LeaderLatchListener
    {
        private final StandardJanusGraph janusGraph;
        private final CuratorFramework client;
        private PathChildrenCache childrenChanage;

        public JanusgraphLeaderLatchListener(StandardJanusGraph janusGraph, CuratorFramework client) {
            this.janusGraph = janusGraph;
            this.client = client;
        }

        @Override
        public void isLeader() {
            if(childrenChanage!=null){
                try {
                    childrenChanage.close();
                } catch (IOException exception) {
                }
            }
            if(janusGraph!=null&&janusGraph.isOpen()){
                this.disposeInstance();
            }
            childrenChanage= new PathChildrenCache(client, "/"+LeaderLatchAdapter.INSTANCENODE, true);
            childrenChanage.getListenable().addListener(
                new PathChildrenCacheListener() {
                    @Override
                    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
                        throws Exception {
                        switch (event.getType()) {
                            case CHILD_REMOVED:
                                this.disposeInstances(client,event);
                                break;
                            case CHILD_ADDED:
                                this.disposeInstances(client,event);
                                break;
                            default:
                                break;
                        }
                    }
                    private void disposeInstances(CuratorFramework client, PathChildrenCacheEvent event){
                        if(janusGraph!=null&&janusGraph.isOpen()&&client.getState()== CuratorFrameworkState.STARTED) {
                            if (event.getData() != null && event.getData().getData() != null) {
                                JanusgraphLeaderLatchListener.this.disposeInstance();
                            }
                        }
                    }
                }
            );
            try {
                childrenChanage.start();
            } catch (Exception exception) {
            }
        }

        @Override
        public void notLeader() {
            if(childrenChanage!=null){
                try {
                    childrenChanage.close();
                } catch (IOException exception) {
                }
            }
        }
        private void disposeInstance() {
            if (client.getState() == CuratorFrameworkState.STARTED) {
                ManagementSystem janusGraphManagement = null;
                try {
                    Set<String> zookeeperInstances = client.getChildren().forPath("/" + LeaderSelectorAdapter.INSTANCENODE).stream().collect(Collectors.toSet());
                    janusGraphManagement = (ManagementSystem) janusGraph.openManagement();
                    boolean chanageClose = false;
                    Set<String> openInstances = janusGraphManagement.getOpenInstancesInternal();
                    for (String openInstance : openInstances) {
                        if (!zookeeperInstances.contains(openInstance)) {
                            janusGraphManagement.forceCloseInstance(openInstance);
                            chanageClose = true;
                        }
                    }
                    if (chanageClose) {
                        janusGraphManagement.commit();
                    } else {
                        janusGraphManagement.rollback();
                    }
                } catch (Exception exception) {
                    if (janusGraphManagement != null && janusGraphManagement.isOpen()) {
                        janusGraphManagement.rollback();
                    }
                }
            }
        }
    }
}
