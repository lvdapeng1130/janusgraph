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
package org.janusgraph.dsl.step;

import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.janusgraph.graphdb.vertices.AbstractVertex;
import org.janusgraph.kydsj.serialize.MediaData;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AttachmentsStep extends FlatMapStep<Element, MediaData> implements AutoCloseable, Configuring {

    protected Parameters parameters = new Parameters();
    private String [] keys;

    public AttachmentsStep(final Traversal.Admin traversal,String [] keys) {
        super(traversal);
        this.keys=keys;
    }

    @Override
    public Parameters getParameters() {
        return this.parameters;
    }

    @Override
    public void configure(final Object... keyValues) {
        this.parameters.set(null, keyValues);
    }

    @Override
    protected Iterator<MediaData> flatMap(final Traverser.Admin<Element> traverser) {
        Element element = traverser.get();
        if(element instanceof AbstractVertex){
            AbstractVertex vertex=(AbstractVertex)element;
            Iterator<MediaData> attachments =  vertex.attachments();
            return attachments;
        }
        Iterator iterator = EmptyIterator.INSTANCE;
        return iterator;
    }


    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public void close() throws Exception {
        closeIterator();
    }
}
