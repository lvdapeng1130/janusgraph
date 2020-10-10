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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.GraphFilterAware;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.hadoop.formats.util.input.current.KGJanusGraphHadoopSetupImpl;

import java.io.IOException;
import java.util.List;


public abstract class KGHadoopInputFormat extends InputFormat<NullWritable, VertexWritable> implements Configurable, GraphFilterAware {

    private final InputFormat<StaticBuffer, Iterable<Entry>> inputFormat;
    private static final HadoopInputFormat.RefCountedCloseable<JanusGraphVertexDeserializer> refCounter;

    static {
        refCounter = new HadoopInputFormat.RefCountedCloseable<>((conf) ->
            new JanusGraphVertexDeserializer(new KGJanusGraphHadoopSetupImpl(conf)));
    }

    public KGHadoopInputFormat(InputFormat<StaticBuffer, Iterable<Entry>> inputFormat) {
        this.inputFormat = inputFormat;
        Preconditions.checkState(Configurable.class.isAssignableFrom(inputFormat.getClass()));
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        return inputFormat.getSplits(context);
    }

    @Override
    public RecordReader<NullWritable, VertexWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new HadoopRecordReader(refCounter, inputFormat.createRecordReader(split, context));
    }

    @Override
    public void setConf(final Configuration conf) {
        ((Configurable)inputFormat).setConf(conf);

        refCounter.setBuilderConfiguration(conf);
    }

    @Override
    public Configuration getConf() {
        return ((Configurable)inputFormat).getConf();
    }

    @Override
    public void setGraphFilter(final GraphFilter graphFilter) {
        // do nothing -- loaded via configuration
    }
}
