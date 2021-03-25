package org.janusgraph.kggraph;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.kydsj.serialize.Note;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 测试对象注释
 */
public class NoteTest extends AbstractKGgraphTest{
    private static final Logger LOGGER = LoggerFactory.getLogger(NoteTest.class);


    /**
     * 给顶点对象插入注释
     */
    @Test
    public void insertNote(){
        LOGGER.info("创建一个顶点并添加一个注释");
        String tid="tid001";
        Note note=new Note("我是注释的id");
        note.setId("我是注释的id");
        note.setNoteTitle("我是注释的标题");
        note.setNoteData("我是注释的内容");
        note.setDsr(Sets.newHashSet("我是注释的dsr"));

        //为顶点添加一个注释
        final Vertex mediaAndNote=g.addV("object_qq")
            .property(T.id, tid)
            .property(BaseKey.VertexNote.name(),note)
            .next();
        g.tx().commit();
    }

    @Test
    public void insertNoteByDsl(){
        LOGGER.info("创建一个顶点并添加一个注释");
        String tid="tid002";
        Note note=new Note("我是注释的iddsl");
        note.setId("我是注释的iddsl");
        note.setNoteTitle("我是注释的标题dsl");
        note.setNoteData("我是注释的内容dsl");
        note.setDsr(Sets.newHashSet("我是注释的dsrdsl"));
        g.V().properties();

        //为顶点添加一个注释
        final Vertex mediaAndNote=g.addV("object_qq")
            .property(T.id, tid).note(note)
            .next();
        g.tx().commit();
    }

    /**
     * 给顶点对象插入注释(另一个注释）
     */
    @Test
    public void insertOtherNote(){
        LOGGER.info("创建一个顶点并添加一个注释");
        String tid="tid001";
        Note note=new Note("我是注释的2");
        note.setId("我是注释的2");
        note.setNoteTitle("我是注释的标题2");
        note.setNoteData("我是注释的内容2");
        note.setDsr(Sets.newHashSet("我是注释的dsr"));

        //为顶点添加一个注释
        final Vertex mediaAndNote=g.addV("object_qq")
            .property(T.id, tid)
            .property(BaseKey.VertexNote.name(),note)
            .next();
        g.tx().commit();
    }


    /**
     * 根据tid获取对象的注释
     */
    @Test
    public void readNotes() {
        String tid="tid002";
        String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
        List<Note> notes = this.getJanusGraph().getNotes(graphId);
        LOGGER.info("读取到的注释------------------------------------");
        if(notes!=null) {
            LOGGER.info("查询到的注释数量是=>"+notes.size());
            for (Note note : notes) {
                LOGGER.info(note.toString());
                LOGGER.info("--------------------------------------------");
            }
        }
    }

    /**
     * 查询注释by gremlin
     */
    @Test
    public void readNotesByDsl() {
        String tid="tid002";
        String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
        List<Note> notes= g.V(graphId).notes().toList();
        LOGGER.info("读取到的注释------------------------------------");
        if(notes!=null) {
            LOGGER.info("查询到的注释数量是=>"+notes.size());
            for (Note note : notes) {
                LOGGER.info(note.toString());
                LOGGER.info("--------------------------------------------");
            }
        }
    }
    /**
     * 查询注释by gremlin
     */
    @Test
    public void readNotesByDslKey() {
        String tid="tid001";
        String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
        List<Note> notes= g.V(graphId).notes("我是注释的iddsl").toList();
        LOGGER.info("读取到的注释------------------------------------");
        if(notes!=null) {
            LOGGER.info("查询到的注释数量是=>"+notes.size());
            for (Note note : notes) {
                LOGGER.info(note.toString());
                LOGGER.info("--------------------------------------------");
            }
        }
    }

    /**
     * 删除指定对象的指定注释
     */
    @Test
    public void dropNoteByDsl(){
        String tid="tid001";
        String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
        List<Note> notes = g.V(graphId).notes("我是注释的iddsl").dropExpand().toList();
        g.tx().commit();
    }


}
