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
package org.janusgraph.dsl;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.GremlinDsl;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.GremlinDsl.AnonymousMethod;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.dsl.step.AddAttachmentStep;
import org.janusgraph.dsl.step.AddNoteStep;
import org.janusgraph.dsl.step.AttachmentsStep;
import org.janusgraph.dsl.step.NotesStep;
import org.janusgraph.kydsj.serialize.MediaData;
import org.janusgraph.kydsj.serialize.Note;

/**
 * This Social DSL is meant to be used with the TinkerPop "modern" toy graph.
 * <p/>
 * All DSLs should extend {@code GraphTraversal.Admin} and be suffixed with "TraversalDsl". Simply add DSL traversal
 * methods to this interface. Use Gremlin's steps to build the underlying traversal in these methods to ensure
 * compatibility with the rest of the TinkerPop stack and provider implementations.
 * <p/>
 * Arguments provided to the {@code GremlinDsl} annotation are all optional. In this case, a {@code traversalSource} is
 * specified which points to a specific implementation to use. Had that argument not been specified then a default
 * {@code TraversalSource} would have been generated.
 */
@GremlinDsl(traversalSource = "org.janusgraph.dsl.KydsjTraversalSourceDsl")
public interface KydsjTraversalDsl<S, E> extends GraphTraversal.Admin<S, E> {

    /**
     * 给对象添加注释信息
     * @param note
     * @return
     */
    public default GraphTraversal<S, Vertex> note(Note note){
        this.asAdmin().getBytecode().addStep(Symbols.note,note);
        final AddNoteStep<Element> addNoteStep = new AddNoteStep<>(this.asAdmin(), note);
        return (KydsjTraversalDsl)this.asAdmin().addStep(addNoteStep);
    }

    /**
     * 给对象添加附件信息
     * @param mediaData
     * @return
     */
    public default GraphTraversal<S, Vertex> attachment(MediaData mediaData){
        this.asAdmin().getBytecode().addStep(Symbols.attachment,mediaData);
        final AddAttachmentStep<Element> addAttachmentStep = new AddAttachmentStep<>(this.asAdmin(), mediaData);
        return (KydsjTraversalDsl)this.asAdmin().addStep(addAttachmentStep);
    }

    /**
     * 查询对象的所有注释
     * @return
     */
    public default GraphTraversal<S,Note> notes() {
        this.asAdmin().getBytecode().addStep(Symbols.notes);
        return this.asAdmin().addStep(new NotesStep(this.asAdmin()));
    }

    /**
     * 查询对象的所有附件
     * @return
     */
    public default GraphTraversal<S,MediaData> attachments() {
        this.asAdmin().getBytecode().addStep(Symbols.attachments);
        return this.asAdmin().addStep(new AttachmentsStep(this.asAdmin()));
    }



    /**
     * From a {@code Vertex} traverse "knows" edges to adjacent "person" vertices and filter those vertices on the
     * "name" property.
     *
     * @param personName the name of the person to filter on
     */
    public default GraphTraversal<S, Vertex> knows(String personName) {
        return ((KydsjTraversalDsl) out("knows")).person().has("name", personName);
    }

    /**
     * From a {@code Vertex} traverse "knows" edges to adjacent "person" vertices and determine the youngest age of
     * those persons.
     */
    public default <E2 extends Number> GraphTraversal<S, E2> youngestFriendsAge() {
        return ((KydsjTraversalDsl) out("knows")).person().values("age").min();
    }

    /**
     * Designed to be used as a filter for "person" vertices based on the number of "created" edges encountered.
     *
     * @param number the minimum number of projects a person created
     */
    public default GraphTraversal<S, Long> createdAtLeast(int number) {
        return outE("created").count().is(P.gte(number));
    }

    /**
     * Filters objects by the "person" label. This step is designed to work with incoming vertices.
     */
    @AnonymousMethod(returnTypeParameters = {"A", "A"}, methodTypeParameters = {"A"})
    public default GraphTraversal<S, E> person() {
        return hasLabel("person");
    }

    public static final class Symbols {
        private Symbols() {
            // static fields only
        }

        public static final String note = "note";
        public static final String notes = "notes";
        public static final String attachment = "attachment";
        public static final String attachments = "attachments";
    }
}
