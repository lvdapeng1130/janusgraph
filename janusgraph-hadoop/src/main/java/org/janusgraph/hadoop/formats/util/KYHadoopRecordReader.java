// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.hadoop.formats.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.janusgraph.diskstorage.PropertyEntry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.vertices.PreloadedVertex;
import org.janusgraph.kydsj.serialize.MediaData;
import org.janusgraph.kydsj.serialize.Note;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (https://markorodriguez.com)
 */
public class KYHadoopRecordReader extends RecordReader<NullWritable, VertexWritable> {

    private final RecordReader<StaticBuffer, PropertyEntry> reader;
    private final KYHadoopInputFormat.RefCountedCloseable countedDeserializer;
    private JanusGraphVertexDeserializer deserializer;
    private VertexWritable vertex;
    private GraphFilter graphFilter;

    public KYHadoopRecordReader(final KYHadoopInputFormat.RefCountedCloseable<JanusGraphVertexDeserializer> countedDeserializer,
                                final RecordReader<StaticBuffer, PropertyEntry> reader) {
        this.countedDeserializer = countedDeserializer;
        this.reader = reader;
        this.deserializer = countedDeserializer.acquire();
    }

    @Override
    public void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        reader.initialize(inputSplit, taskAttemptContext);

        final Configuration conf = taskAttemptContext.getConfiguration();
        if (conf.get(Constants.GREMLIN_HADOOP_GRAPH_FILTER, null) != null) {
            graphFilter = VertexProgramHelper.deserialize(ConfUtil.makeApacheConfiguration(conf),
                Constants.GREMLIN_HADOOP_GRAPH_FILTER);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (reader.nextKeyValue()) {
            // TODO janusgraph05 integration -- the duplicate() call may be unnecessary
            final StarGraph.StarVertex starVertex =
                    deserializer.readStarVertex(reader.getCurrentKey(), reader.getCurrentValue());
            if (null != starVertex) {
                vertex = new VertexWritable();
                vertex.set(starVertex);
                if (graphFilter == null) {
                    return true;
                } else {
                    final Optional<StarGraph.StarVertex> vertexWritable = vertex.get().applyGraphFilter(graphFilter);
                    if (vertexWritable.isPresent()) {
                        vertex.set(vertexWritable.get());
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static StarGraph.StarVertex ofStarVertex(final PreloadedVertex vertex) {
        // else convert to a star graph
        final StarGraph starGraph = StarGraph.open();
        final StarGraph.StarVertex starVertex = (StarGraph.StarVertex) starGraph.addVertex(T.id, vertex.id(), T.label, vertex.label());

        final boolean supportsMetaProperties = vertex.graph().features().vertex().supportsMetaProperties();

        vertex.properties().forEachRemaining(vp -> {
            final VertexProperty<?> starVertexProperty = starVertex.property(VertexProperty.Cardinality.list, vp.key(), vp.value(), T.id, vp.id());
            if (supportsMetaProperties)
                vp.properties().forEachRemaining(p -> starVertexProperty.property(p.key(), p.value()));
        });
        vertex.edges(Direction.IN).forEachRemaining(edge -> {
            Vertex outVertex = starGraph.addVertex(T.id, edge.outVertex().id(),T.label,edge.outVertex().label());
            final Edge starEdge =outVertex.addEdge(edge.label(),starVertex,T.id, edge.id());
            edge.properties().forEachRemaining(p -> starEdge.property(p.key(), p.value()));
        });

        vertex.edges(Direction.OUT).forEachRemaining(edge -> {
            Vertex inVertex=starGraph.addVertex(T.id, edge.inVertex().id(),T.label,edge.inVertex().label());
            final Edge starEdge=starVertex.addEdge(edge.label(), inVertex,T.id, edge.id());
            edge.properties().forEachRemaining(p -> starEdge.property(p.key(), p.value()));
        });
        //附件
        Iterator<MediaData> mediaIterator = vertex.getMediaIterator();
        if(mediaIterator!=null){
            while (mediaIterator.hasNext()){
                MediaData mediaData = mediaIterator.next();
                starVertex.property(VertexProperty.Cardinality.list, BaseKey.VertexAttachment.name(), mediaData, T.id, mediaData.id());
            }
        }
        //注释
        Iterator<Note> noteIterator = vertex.getNoteIterator();
        if(noteIterator!=null){
            while (noteIterator.hasNext()){
                Note note = noteIterator.next();
                starVertex.property(VertexProperty.Cardinality.list, BaseKey.VertexNote.name(), note, T.id, note.getId());
            }
        }
        return starGraph.getStarVertex();
    }


    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public VertexWritable getCurrentValue() {
        return vertex;
    }

    @Override
    public void close() throws IOException {
        try {
            deserializer = null;
            countedDeserializer.release();
        } catch (Exception e) {
            throw new IOException(e);
        }
        reader.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }
}
