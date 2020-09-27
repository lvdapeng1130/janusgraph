package org.janusgraph.graphdb.database.leader;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LeaderSelectorAdapter extends LeaderSelectorListenerAdapter implements Closeable {
    private static final Logger log =
        LoggerFactory.getLogger(LeaderSelectorAdapter.class);
    public static final String INSTANCENODE="instance";
    public static final String LEADERNODE="/leader";
    private final String name;
    private final LeaderSelector leaderSelector;
    private final StandardJanusGraph janusGraph;
    private final CuratorFramework client;

    public LeaderSelectorAdapter(CuratorFramework client, String name,StandardJanusGraph janusGraph) {
        this.client=client;
        this.janusGraph=janusGraph;
        this.name = name;
        leaderSelector = new LeaderSelector(client, LEADERNODE, this);
        leaderSelector.autoRequeue();
    }

    public void start() throws IOException {
        leaderSelector.start();
    }

    @Override
    public void close() throws IOException {
        leaderSelector.close();
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        if(janusGraph!=null&&janusGraph.isOpen()){
            this.disposeInstance();
        }
        PathChildrenCache childrenChanage= new PathChildrenCache(client, "/"+LeaderSelectorAdapter.INSTANCENODE, true);
        childrenChanage.start();
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
                            LeaderSelectorAdapter.this.disposeInstance();
                        }
                    }
                }
            }
        );
        log.info(String.format("%s is now the master.",name));
        while (true) {
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(100));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void disposeInstance(){
        if(client.getState()== CuratorFrameworkState.STARTED) {
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
                try {
                    if (janusGraphManagement != null && janusGraphManagement.isOpen()) {
                        janusGraphManagement.rollback();
                    }
                } catch (Exception e) {
                }
            }
        }
    }
}