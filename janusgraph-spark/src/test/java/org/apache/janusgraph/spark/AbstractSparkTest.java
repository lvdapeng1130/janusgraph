/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.janusgraph.spark;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.janusgraph.spark.structure.Spark;
import org.apache.janusgraph.spark.structure.io.gryo.GryoSerializer;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPools;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShimServiceLoader;
import org.janusgraph.core.JanusGraph;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractSparkTest {
    private static final Logger logger = LoggerFactory.getLogger(AbstractSparkTest.class);

    @After
    @Before
    public void setupTest() {
        Spark.close();
        HadoopPools.close();
        KryoShimServiceLoader.close();
        logger.info("SparkContext has been closed for " + this.getClass().getCanonicalName() + "-setupTest");
    }

    protected Configuration getBaseConfiguration() {
        final BaseConfiguration configuration = new BaseConfiguration();
        configuration.setProperty(SparkLauncher.SPARK_MASTER, "local[4]");
        configuration.setProperty(Constants.SPARK_SERIALIZER, GryoSerializer.class.getCanonicalName());
        configuration.setProperty(Constants.SPARK_KRYO_REGISTRATION_REQUIRED, true);
        configuration.setProperty(Graph.GRAPH, JanusGraph.class.getName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
        return configuration;
    }
}
