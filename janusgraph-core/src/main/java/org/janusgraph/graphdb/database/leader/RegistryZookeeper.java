package org.janusgraph.graphdb.database.leader;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * @author: ldp
 * @time: 2020/8/18 9:22
 * @jira:
 */
@Slf4j
public class RegistryZookeeper implements Closeable {
    private final GraphDatabaseConfiguration configuration;
    private final String uniqueInstanceId;
    private final StandardJanusGraph janusGraph;
    private RetryPolicy retryPolicy;
    private CuratorFramework curatorClient;
    private LeaderSelectorAdapter leaderSelectorAdapter;

    public RegistryZookeeper(GraphDatabaseConfiguration configuration, String uniqueInstanceId,StandardJanusGraph janusGraph) {
        this.configuration = configuration;
        this.uniqueInstanceId = uniqueInstanceId;
        this.janusGraph=janusGraph;
    }

    public void registry(){
        //连接zookeeper
        //String zookeeperURI = configuration.getConfiguration().get(JANUSGRAPH_ZOOKEEPER_URI);
        String[] zookeeperURIs = configuration.getConfiguration().get(JANUSGRAPH_ZOOKEEPER_URI);
        String zookeeperNamespace = configuration.getConfiguration().get(JANUSGRAPH_ZOOKEEPER_NAMESPACE);
        int sessionTimeoutMs = configuration.getConfiguration().get(ZOOKEEPER_SESSIONTIMEOUTMS);
        int connectionTimeoutMs = configuration.getConfiguration().get(ZOOKEEPER_CONNECTIONTIMEOUTMS);
        String graph_node = configuration.getConfiguration().get(GRAPH_NODE);
        boolean registry_zookeeper_enable = configuration.getConfiguration().get(REGISTRY_ZOOKEEPER_ENABLE);
        if(registry_zookeeper_enable&&zookeeperURIs!=null&&zookeeperURIs.length>0&&StringUtils.isNotBlank(zookeeperNamespace)&&StringUtils.isNotBlank(graph_node)) {
            String zookeeperURI = Arrays.stream(zookeeperURIs).collect(Collectors.joining(","));
            log.info(String.format("连接zookeeper->%s (%s/%s)",zookeeperURI,zookeeperNamespace,graph_node));
            String rootPath=zookeeperNamespace+"/"+graph_node;
            retryPolicy = new ExponentialBackoffRetry(1000, 3);
            curatorClient = CuratorFrameworkFactory.builder()
                .connectString(zookeeperURI)
                .sessionTimeoutMs(sessionTimeoutMs)
                .connectionTimeoutMs(connectionTimeoutMs)
                .namespace(rootPath)
                .retryPolicy(retryPolicy)
                .build();
            curatorClient.start();
            this.createEphemeralNode(uniqueInstanceId);
            this.leaderSelectorAdapter = new LeaderSelectorAdapter(curatorClient, String.format("graph %s,Client #%s",graph_node, uniqueInstanceId),this.janusGraph);
            try {
                leaderSelectorAdapter.start();
            } catch (Exception exception) {
            }
        }
    }

    private void createEphemeralNode(String uniqueInstanceId){
        if(curatorClient!=null){
            try {
                String instancePath="/"+LeaderSelectorAdapter.INSTANCENODE+"/"+uniqueInstanceId;
                curatorClient.create()
                    .creatingParentContainersIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(instancePath,uniqueInstanceId.getBytes());
            } catch (Exception exception) {
            }
        }
    }

    @Override
    public void close() throws IOException {
        //关闭zookeeper连接
        if(leaderSelectorAdapter !=null){
            leaderSelectorAdapter.close();
        }
        if(curatorClient!=null) {
            curatorClient.close();
        }
    }
}
