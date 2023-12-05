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

package org.janusgraph.diskstorage.util;

import org.janusgraph.core.TransactionCustom;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.idassigner.Preconditions;

import java.time.Instant;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Dan LaRocque (dalaro@hopcount.org)
 */
public class StandardBaseTransactionConfig implements BaseTransactionConfig {

    private volatile Instant commitTime;
    private final TimestampProvider times;
    private final String groupName;
    private final Configuration customOptions;
    private boolean indexMode;
    private Set<String> skipIndexes;

    private TransactionCustom transactionCustom;

    private StandardBaseTransactionConfig(String groupName,
                                          TimestampProvider times,
                                          Instant commitTime,
                                          Configuration customOptions,boolean indexMode,
                                          TransactionCustom transactionCustom,Set<String> skipIndexes) {
        Preconditions.checkArgument(customOptions!=null);
        Preconditions.checkArgument(null != times || null != commitTime);
        this.groupName = groupName;
        this.times = times;
        this.commitTime = commitTime;
        this.customOptions = customOptions;
        this.indexMode=indexMode;
        this.skipIndexes=skipIndexes;
        this.transactionCustom=transactionCustom;
    }

    @Override
    public synchronized Instant getCommitTime() {
        if (commitTime==null) {
            //set commit time to current time
            commitTime = times.getTime();
        }
        return commitTime;
    }

    @Override
    public synchronized void setCommitTime(Instant time) {
        Preconditions.checkArgument(commitTime==null,"A commit time has already been set");
        this.commitTime=time;
    }

    @Override
    public boolean hasCommitTime() {
        return commitTime!=null;
    }

    @Override
    public TimestampProvider getTimestampProvider() {
        return times;
    }

    @Override
    public boolean hasGroupName() {
        return groupName !=null;
    }

    @Override
    public String getGroupName() {
        return groupName;
    }

    @Override
    public <V> V getCustomOption(ConfigOption<V> opt) {
        return customOptions.get(opt);
    }

    @Override
    public Configuration getCustomOptions() {
        return customOptions;
    }

    @Override
    public boolean isIndexMode() {
        return indexMode;
    }

    @Override
    public Set<String> getSkipIndexes(){
        return this.skipIndexes;
    }

    @Override
    public TransactionCustom getTransactionCustom() {
        return transactionCustom;
    }

    public static class Builder {

        private Instant commitTime = null;
        private TimestampProvider times;
        private String groupName = GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT;
        private Configuration customOptions = Configuration.EMPTY;
        private boolean hasIndexAddMode;
        private Set<String> skipIndexes;

        private TransactionCustom transactionCustom;

        public Builder() { }

        /**
         * Copies everything from {@code template} to this builder except for
         * the {@code commitTime}.
         *
         * @param template
         *            an existing transaction from which this builder will take
         *            its values
         */
        public Builder(BaseTransactionConfig template) {
            customOptions(template.getCustomOptions());
            groupName(template.getGroupName());
            timestampProvider(template.getTimestampProvider());
            indexMode(template.isIndexMode());
            skipIndexes(template.getSkipIndexes());
            setTransactionCustom(template.getTransactionCustom());
        }

        public Builder indexMode(boolean hasIndexMode) {
            this.hasIndexAddMode = hasIndexMode;
            return this;
        }

        public Builder skipIndexes(Set<String> skipIndexes) {
            this.skipIndexes = skipIndexes;
            return this;
        }

        public Builder setTransactionCustom(TransactionCustom transactionCustom) {
            this.transactionCustom = transactionCustom;
            return this;
        }

        public Builder groupName(String group) {
            groupName = group;
            return this;
        }

        public Builder commitTime(Instant commit) {
            commitTime = commit;
            return this;
        }

        public Builder timestampProvider(TimestampProvider times) {
            this.times = times;
            return this;
        }

        public Builder customOptions(Configuration c) {
            customOptions = c;
            Preconditions.checkNotNull(customOptions, "Null custom options disallowed; use an empty Configuration object instead");
            return this;
        }

        public StandardBaseTransactionConfig build() {
            return new StandardBaseTransactionConfig(groupName, times, commitTime, customOptions,hasIndexAddMode,transactionCustom,skipIndexes);
        }
    }

    public static StandardBaseTransactionConfig of(TimestampProvider times) {
        return new Builder().timestampProvider(times).build();
    }

    public static StandardBaseTransactionConfig of(TimestampProvider times, Configuration customOptions) {
        return new Builder().timestampProvider(times).customOptions(customOptions).build();
    }

}
