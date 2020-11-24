/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.janusgraph.spark.structure.io;

import org.apache.janusgraph.hadoop.structure.io.AbstractIoRegistryCheck;
import org.apache.janusgraph.spark.computer.SparkJanusGraphComputer;
import org.apache.janusgraph.spark.structure.Spark;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPools;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShimServiceLoader;
import org.janusgraph.core.JanusGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SparkIoRegistryCheck extends AbstractIoRegistryCheck {

    @Before
    public void setup() throws Exception {
        super.setup();
        SparkContextStorage.open("local[4]");
        Spark.close();
        HadoopPools.close();
        KryoShimServiceLoader.close();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        Spark.create("local[4]");
        Spark.close();
        HadoopPools.close();
        KryoShimServiceLoader.close();
    }

    @Test
    public void shouldSupportGryoV1d0IoRegistry() throws Exception {
        super.checkGryoV1d0IoRegistryCompliance((JanusGraph) graph, SparkJanusGraphComputer.class);
    }

    @Test
    public void shouldSupportGryoV3d0IoRegistry() throws Exception {
        super.checkGryoV3d0IoRegistryCompliance((JanusGraph) graph, SparkJanusGraphComputer.class);
    }

    @Test
    public void shouldSupportGraphSONIoRegistry() throws Exception {
        super.checkGraphSONIoRegistryCompliance((JanusGraph) graph, SparkJanusGraphComputer.class);
    }
}
