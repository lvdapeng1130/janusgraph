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

package org.janusgraph.graphdb.transaction.lock;

import org.janusgraph.graphdb.database.idassigner.Preconditions;
import org.janusgraph.diskstorage.util.time.Timer;
import org.janusgraph.diskstorage.util.time.TimestampProvider;

import java.time.Duration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class CombinerLock implements TransactionLock {

    private final TransactionLock first;
    private final TransactionLock second;
    private final TimestampProvider times;

    public CombinerLock(final TransactionLock first, final TransactionLock second, TimestampProvider times) {
        this.first = Preconditions.checkNotNull(first);
        this.second = Preconditions.checkNotNull(second);
        this.times = Preconditions.checkNotNull(times);
    }

    @Override
    public void lock(Duration timeout) {
        Timer t = times.getTimer().start();
        first.lock(timeout);
        Duration remainingTimeout = timeout.minus(t.elapsed());
        try {
            second.lock(remainingTimeout);
        } catch (RuntimeException e) {
            first.unlock();
            throw e;
        }
    }

    @Override
    public void unlock() {
        try {
            first.unlock();
        } finally {
            second.unlock();
        }

    }

    @Override
    public boolean inUse() {
        return first.inUse() || second.inUse();
    }
}
