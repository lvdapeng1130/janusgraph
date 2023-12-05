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

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.kydsj.serialize.Note;
import org.junit.Test;

import java.util.Date;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KydsjDslTest {

    private Graph graph = TinkerFactory.createModern();
    private KydsjTraversalSource social = traversal(KydsjTraversalSource.class).withGraph(graph);


    @Test
    public void testNote(){
        String tid="tid001";
        Note note=new Note("我是注释的2");
        note.setId("我是注释的2");
        note.setNoteTitle("我是注释的标题2");
        note.setNoteData("我是注释的内容2");
        note.setDsr(Sets.newHashSet("我是注释的dsr"));
        Vertex next = social.addV("person")
            .property("name", "我是测试qq",
                "startDate", new Date(),
                "endDate", new Date(),
                "dsr", "程序导入1",
                "geo", Geoshape.point(22.22, 113.1122))
            .property("tid", tid)
            .property("qq_num", "111111", "dsr", "程序导入")
            .property(T.id, 100).note(note).next();
        System.out.println(next);
    }

    @Test
    public void shouldValidateThatMarkoKnowsJosh() {
        assertTrue(social.V().has("name","marko").knows("josh").hasNext());
        assertTrue(social.persons("marko").knows("josh").hasNext());
    }

    @Test
    public void shouldGetAgeOfYoungestFriendOfMarko() {
        assertEquals(27, social.V().has("name","marko").youngestFriendsAge().next().intValue());
        assertEquals(27, social.persons("marko").youngestFriendsAge().next().intValue());
    }

    @Test
    public void shouldFindAllPersons() {
        assertEquals(4, social.persons().count().next().intValue());
    }

    @Test
    public void shouldFindAllPersonsWithTwoOrMoreProjects() {
        assertEquals(1, social.persons().filter(__.createdAtLeast(2)).count().next().intValue());
    }
}