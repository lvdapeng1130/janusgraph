package org.janusgraph.kggraph;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.kggraph.bo.GraphVertex;

import java.util.Iterator;

public class TransformGraphHandler {
    public GraphVertex transform(final Vertex vertex){
        GraphVertex graphVertex=new GraphVertex();
        graphVertex.setId(vertex.id().toString());
        graphVertex.setType(vertex.label());
        if (vertex.keys().size() == 0) {
            for (String key : vertex.keys()) {
                final Iterator<VertexProperty<Object>> vertexProperties = vertex.properties(key);
               /* if (vertexProperties.hasNext()) {
                    jsonGenerator.writeFieldName(key);

                    jsonGenerator.writeStartArray();
                    while (vertexProperties.hasNext()) {
                        jsonGenerator.writeObject(vertexProperties.next());
                    }
                    jsonGenerator.writeEndArray();
                }*/
            }

        }
        return graphVertex;
    }
}
