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

package org.janusgraph.graphdb.hbase;

import org.janusgraph.HBaseStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class HBaseGraphTest extends JanusGraphTest {
    @BeforeAll
    public static void startHBase() throws IOException {
       // HBaseStorageSetup.startHBase();
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return HBaseStorageSetup.getHBaseConfiguration("ldp_test_graph").getConfiguration();
    }

    @Override @Test @Disabled("HBase does not support retrieving cell TTL by client")
    public void testVertexTTLImplicitKey() { }

    @Override @Test @Disabled("HBase does not support retrieving cell TTL by client")
    public void testEdgeTTLImplicitKey() { }

}
