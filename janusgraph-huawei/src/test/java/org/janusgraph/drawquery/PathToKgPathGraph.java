package org.janusgraph.drawquery;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.drawquery.pojo.KgDiagram;
import org.janusgraph.drawquery.pojo.KgLink;
import org.janusgraph.drawquery.pojo.KgNode;
import org.janusgraph.drawquery.pojo.KgPath;
import org.janusgraph.drawquery.pojo.KgPathGraph;
import org.janusgraph.qq.DefaultPropertyKey;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

public class PathToKgPathGraph implements Callable<KgPathGraph> {
    private List<Path> paths;
    public PathToKgPathGraph(List<Path> paths){
        this.paths=paths;
    }
    @Override
    public KgPathGraph call() throws Exception {
        KgPathGraph pathGraph = new KgPathGraph();
        KgDiagram diagram = new KgDiagram();
        Set<String> set = Sets.newHashSet();
        for(Path path : paths) {
            KgPath kgPath = new KgPath();
            List<Object> objects = path.objects();
            for (Object o : objects) {
                if (o instanceof Vertex) {
                    Vertex v = (Vertex) o;
                    String tid = v.id().toString();
                    if (set.contains(tid)) {
                        continue;
                    }
                    set.add(tid);
                    String text = v.property(DefaultPropertyKey.TITLE.getKey()).orElse("----").toString();
                    String label = v.label();
                    KgNode node = new KgNode();
                    node.setKey(tid);
                    node.setText(text);
                    node.setType(label);
                    diagram.getNodeDataArray().add(node);
                    kgPath.getLabels().add(text);
                } else if (o instanceof Edge) {
                    Edge e = (Edge) o;
                    String linkId = e.id().toString();
                    if (set.contains(linkId)) {
                        continue;
                    }
                    set.add(linkId);
                    String from = e.outVertex().id().toString();
                    String to = e.inVertex().id().toString();
                    String text = e.property(DefaultPropertyKey.LINK_TEXT.getKey()).orElse("").toString();
                    String label = e.label();
                    KgLink link = new KgLink();
                    link.setFrom(from);
                    link.setKey(linkId);
                    link.setText(text);
                    link.setTo(to);
                    link.setType(label);
                    diagram.getLinkDataArray().add(link);
                    kgPath.getLinks().add(linkId);
                }
            }
            pathGraph.getKgPaths().add(kgPath);
        }
        pathGraph.setKgDiagram(diagram);
        return pathGraph;
    }
}
