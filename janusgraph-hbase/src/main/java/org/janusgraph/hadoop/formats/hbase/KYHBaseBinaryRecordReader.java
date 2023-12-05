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

package org.janusgraph.hadoop.formats.hbase;

import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.PropertyEntry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

public class KYHBaseBinaryRecordReader extends RecordReader<StaticBuffer, PropertyEntry> {

    private final RecordReader<ImmutableBytesWritable, Result> reader;

    private final byte[] edgestoreFamilyBytes;
    private final byte[] ppFamilyBytes;
    private final byte[] mediaFamilyBytes;
    private final byte[] noteFamilyBytes;

    public KYHBaseBinaryRecordReader(final RecordReader<ImmutableBytesWritable, Result> reader, final byte[] edgestoreFamilyBytes,final byte[] ppFamilyBytes,final byte[] mediaFamilyBytes,final byte[] noteFamilyBytes) {
        this.reader = reader;
        this.edgestoreFamilyBytes = edgestoreFamilyBytes;
        this.ppFamilyBytes=ppFamilyBytes;
        this.mediaFamilyBytes=mediaFamilyBytes;
        this.noteFamilyBytes=noteFamilyBytes;
    }

    @Override
    public void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        reader.initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return reader.nextKeyValue();
    }

    @Override
    public StaticBuffer getCurrentKey() throws IOException, InterruptedException {
        return StaticArrayBuffer.of(reader.getCurrentKey().copyBytes());
    }

    @Override
    public PropertyEntry getCurrentValue() throws IOException, InterruptedException {
        HBaseMapIterable edgeEntries = new HBaseMapIterable(reader.getCurrentValue().getMap().get(edgestoreFamilyBytes));
        PropertyEntry propertyEntry=new PropertyEntry(edgeEntries);
        if(ppFamilyBytes!=null) {
            NavigableMap<byte[], NavigableMap<Long, byte[]>> propertyPropeties = reader.getCurrentValue().getMap().get(ppFamilyBytes);
            if (propertyPropeties != null&&propertyPropeties.size()>0) {
                HBaseMapIterable ppEntries = new HBaseMapIterable(propertyPropeties);
                propertyEntry.setPropertyProperties(ppEntries);
            }
        }
        if(mediaFamilyBytes!=null) {
            NavigableMap<byte[], NavigableMap<Long, byte[]>> medias = reader.getCurrentValue().getMap().get(mediaFamilyBytes);
            if (medias != null&&medias.size()>0) {
                HBaseMapIterable entries = new HBaseMapIterable(medias);
                propertyEntry.setMedias(entries);
            }
        }
        if(noteFamilyBytes!=null) {
            NavigableMap<byte[], NavigableMap<Long, byte[]>> notes = reader.getCurrentValue().getMap().get(noteFamilyBytes);
            if (notes != null&&notes.size()>0) {
                HBaseMapIterable entries = new HBaseMapIterable(notes);
                propertyEntry.setNotes(entries);
            }
        }
        return propertyEntry;
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return this.reader.getProgress();
    }

    private static class HBaseMapIterable implements Iterable<Entry> {

        private final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnValues;

        public HBaseMapIterable(final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnValues) {
            this.columnValues = columnValues;
        }

        @Override
        public Iterator<Entry> iterator() {
            if(columnValues!=null) {
                return new HBaseMapIterator(columnValues.entrySet().iterator());
            }else{
                return EmptyIterator.INSTANCE;
            }
        }

    }

    private static class HBaseMapIterator implements Iterator<Entry> {

        private final Iterator<Map.Entry<byte[], NavigableMap<Long, byte[]>>> iterator;

        public HBaseMapIterator(final Iterator<Map.Entry<byte[], NavigableMap<Long, byte[]>>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Entry next() {
            final Map.Entry<byte[], NavigableMap<Long, byte[]>> entry = iterator.next();
            byte[] col = entry.getKey();
            byte[] val = entry.getValue().lastEntry().getValue();
            return StaticArrayEntry.of(new StaticArrayBuffer(col), new StaticArrayBuffer(val));
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}

