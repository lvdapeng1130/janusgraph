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

import com.google.common.collect.BiMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.PropertyEntry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.hbase.HBaseStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.hadoop.config.JanusGraphHadoopConfiguration;
import org.janusgraph.hadoop.formats.util.KYAbstractBinaryInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public class KYHBaseBinaryInputFormat extends KYAbstractBinaryInputFormat {

    private static final Logger log = LoggerFactory.getLogger(KYHBaseBinaryInputFormat.class);

    private final TableInputFormat tableInputFormat = new TableInputFormat();
    private RecordReader<ImmutableBytesWritable, Result> tableReader;
    private byte[] edgeStoreFamily;
    private byte[] ppFamilyBytes;
    private byte[] mediaFamilyBytes;
    private byte[] noteFamilyBytes;

    @Override
    public List<InputSplit> getSplits(final JobContext jobContext) throws IOException, InterruptedException {
        return this.tableInputFormat.getSplits(jobContext);
    }

    @Override
    public RecordReader<StaticBuffer, PropertyEntry> createRecordReader(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        tableReader = tableInputFormat.createRecordReader(inputSplit, taskAttemptContext);
        return new KYHBaseBinaryRecordReader(tableReader, edgeStoreFamily,ppFamilyBytes,mediaFamilyBytes,noteFamilyBytes);
    }

    public static Configuration deepCopy(Configuration original) {
        Configuration copy = new Configuration();
        // 拷贝属性
        for (Map.Entry<String, String> entry : original) {
            copy.set(entry.getKey(), entry.getValue());
        }
        return copy;
    }

    @Override
    public void setConf(final Configuration conf) {
        Configuration config=deepCopy(conf);
        HBaseConfiguration.addHbaseResources(config);
        super.setConf(config);

        // Pass the extra pass-through properties directly to HBase/Hadoop config.
        final Map<String, Object> configSub = janusgraphConf.getSubset(HBaseStoreManager.HBASE_CONFIGURATION_NAMESPACE);
        for (Map.Entry<String, Object> entry : configSub.entrySet()) {
            log.info("HBase configuration: setting {}={}", entry.getKey(), entry.getValue());
            if (entry.getValue() == null) continue;
            config.set(entry.getKey(), entry.getValue().toString());
        }

        Boolean isReadMedias = janusgraphConf.get(HBaseStoreManager.READ_MEDIAS);
        Boolean isReadNotes = janusgraphConf.get(HBaseStoreManager.READ_NOTES);
        Boolean isReadPropertyOfProperties = janusgraphConf.get(HBaseStoreManager.READ_PROPERTY_PROPERTIES);
        config.set(TableInputFormat.INPUT_TABLE, janusgraphConf.get(HBaseStoreManager.HBASE_TABLE));
        config.set(HConstants.ZOOKEEPER_QUORUM, janusgraphConf.get(GraphDatabaseConfiguration.STORAGE_HOSTS)[0]);
        if (janusgraphConf.has(GraphDatabaseConfiguration.STORAGE_PORT))
            config.set(HConstants.ZOOKEEPER_CLIENT_PORT, String.valueOf(janusgraphConf.get(GraphDatabaseConfiguration.STORAGE_PORT)));
        config.set("autotype", "none");
        log.debug("hbase.security.authentication={}", config.get("hbase.security.authentication"));
        Scan scanner = new Scan();
        String cfName = mrConf.get(JanusGraphHadoopConfiguration.COLUMN_FAMILY_NAME);
        String cfPPName = Backend.PROPERTY_PROPERTIES;
        String cfMedia =Backend.ATTACHMENT_FAMILY_NAME;
        String cfNote =Backend.NOTE_FAMILY_NAME;
        // TODO the space-saving short name mapping leaks from HBaseStoreManager here
        if (janusgraphConf.get(HBaseStoreManager.SHORT_CF_NAMES)) {
            try {
                final BiMap<String,String> shortCfMap = HBaseStoreManager.createShortCfMap(janusgraphConf);
                cfName = HBaseStoreManager.shortenCfName(shortCfMap, cfName);
                cfPPName = HBaseStoreManager.shortenCfName(shortCfMap, cfPPName);
                cfMedia = HBaseStoreManager.shortenCfName(shortCfMap, cfMedia);
                cfNote = HBaseStoreManager.shortenCfName(shortCfMap, cfNote);
            } catch (PermanentBackendException e) {
                throw new RuntimeException(e);
            }
        }
        edgeStoreFamily = Bytes.toBytes(cfName);
        scanner.addFamily(edgeStoreFamily);
        if(isReadMedias){
            mediaFamilyBytes = Bytes.toBytes(cfMedia);
            scanner.addFamily(mediaFamilyBytes);
        }
        if(isReadNotes){
            noteFamilyBytes = Bytes.toBytes(cfNote);
            scanner.addFamily(noteFamilyBytes);
        }
        if(isReadPropertyOfProperties){
            ppFamilyBytes = Bytes.toBytes(cfPPName);
            scanner.addFamily(ppFamilyBytes);
        }
        scanner.setCacheBlocks(false);

        //scanner.setFilter(getColumnFilter(janusgraphSetup.inputSlice(this.vertexQuery))); // TODO
        //TODO (minor): should we set other options in https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Scan.html for optimization?
        // This is a workaround, to be removed when convertScanToString becomes public in hbase
        // package.
        Method converter;
        try {
            converter = TableMapReduceUtil.class.getDeclaredMethod("convertScanToString", Scan.class);
            converter.setAccessible(true);
            config.set(TableInputFormat.SCAN, (String) converter.invoke(null, scanner));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        this.tableInputFormat.setConf(config);
    }

    @Override
    public Configuration getConf() {
        return tableInputFormat.getConf();
    }
}
