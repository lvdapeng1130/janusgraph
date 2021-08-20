package org.janusgraph.kggraph;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.kydsj.serialize.MediaData;
import org.janusgraph.kydsj.serialize.MediaDataRaw;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 测试对象的附件
 */
public class MediaDataTest extends AbstractKGgraphTest{
    private static final Logger LOGGER = LoggerFactory.getLogger(MediaDataTest.class);

    /**
     * 给顶点对象插入附件
     */
    @Test
    public void insertMediaData(){
        String tid="tid002";
        LOGGER.info("创建一个顶点并添加一个附件");
        MediaData mediaData=new MediaData("我是附件的key");
        mediaData.setFilename("文件名");
        mediaData.setMediaTitle("附件标题");
        mediaData.setKey("我是附件的key");
        mediaData.setMediaData("我是附件的内容".getBytes());
        mediaData.setDsr(Sets.newHashSet("我是附件的一个dsr"));
        //为顶点添加一个附件
        final Vertex mediaAndNote=g.addV("object_qq")
            .property(T.id, tid)
            .property(BaseKey.VertexAttachment.name(),mediaData)
            .next();
        g.tx().commit();
    }

    @Test
    public void insertMediaDataByDsl(){
        String tid="tid002";
        LOGGER.info("创建一个顶点并添加一个附件");
        MediaData mediaData=new MediaData("我是附件的key");
        mediaData.setFilename("文件名");
        mediaData.setMediaTitle("附件标题");
        mediaData.setKey("我是附件的key");
        mediaData.setMediaData("我是附件的内容".getBytes());
        mediaData.setDsr(Sets.newHashSet("我是附件的一个dsr"));
        final Vertex mediaAndNote=g.addV("object_qq")
            .property(T.id, tid).attachment(mediaData)
            .next();
        g.tx().commit();
    }

    /**
     * 给顶点对象插入附件
     */
    @Test
    public void updateMediaData(){
        String tid="tid002";
        LOGGER.info("创建一个顶点并添加一个附件");
        MediaData mediaData=new MediaData("我是附件的key");
        mediaData.setFilename("文件名_新");
        mediaData.setMediaTitle("附件标题_新");
        mediaData.setKey("我是附件的key");
        mediaData.setMediaData(RandomStringUtils.randomAlphabetic(50000).getBytes());
        mediaData.setDsr(Sets.newHashSet("我是附件的一个dsr"));
        //为顶点添加一个附件
        final Vertex mediaAndNote=g.addV("object_qq")
            .property(T.id, tid)
            .property(BaseKey.VertexAttachment.name(),mediaData)
            .next();
        g.tx().commit();
    }

    /**
     * 给顶点对象插入附件(另一个附件)
     */
    @Test
    public void insertMediaDataOther(){
        String tid="tid002";
        LOGGER.info("创建一个顶点并添加一个附件");
        MediaData mediaData=new MediaData("我是附件的key2");
        mediaData.setFilename("文件名2");
        mediaData.setMediaTitle("附件标题2");
        mediaData.setKey("我是附件的key2");
        mediaData.setMediaData("我是附件的内容2".getBytes());
        mediaData.setDsr(Sets.newHashSet("我是附件的一个dsr"));
        //为顶点添加一个附件
        final Vertex mediaAndNote=g.addV("object_qq")
            .property(T.id, tid)
            .property(BaseKey.VertexAttachment.name(),mediaData)
            .next();
        g.tx().commit();
    }

    @Test
    public void batchMediaData(){
        String tid="tid002";
        LOGGER.info("创建一个顶点并添加一个附件");
        for(int i=50;i<100;i++) {
            MediaData mediaData = new MediaData("我是附件的key"+i);
            mediaData.setFilename("文件名_新"+i);
            mediaData.setMediaTitle("附件标题_新"+i);
            mediaData.setKey("我是附件的key"+i);
            mediaData.setMediaData(RandomStringUtils.randomAlphabetic(50000).getBytes());
            mediaData.setDsr(Sets.newHashSet("我是附件的一个dsr"));
            //为顶点添加一个附件
            final Vertex mediaAndNote = g.addV("object_qq")
                .property(T.id, tid)
                .property(BaseKey.VertexAttachment.name(), mediaData)
                .next();
        }
        g.tx().commit();
    }
    /**
     * 根据tid获取对象的附件
     */
    @Test
    public void readMediaData() {
        String tid="tid002";
        String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
        Stopwatch started = Stopwatch.createStarted();
        List<MediaData> mediaDatas = this.getJanusGraph().getMediaDatas(graphId);
        LOGGER.info("读取到的附件------------------------------------>"+mediaDatas.size());
        if(mediaDatas!=null) {
            started.stop();
            LOGGER.info("查询到的附件数量是=>"+mediaDatas.size()+",用时："+started.elapsed(TimeUnit.MILLISECONDS));
            /*for (MediaData mediaData : mediaDatas) {
                LOGGER.info(mediaData.toString());
                LOGGER.info("--------------------------------------------");
            }*/
        }
    }

    @Test
    public void readMediaDataRaw() {
        String tid="tid002";
        String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
        List<MediaData> mediaDatas = this.getJanusGraph().getMediaDatas(graphId);
        LOGGER.info("读取到的附件------------------------------------");
        if(mediaDatas!=null) {
            LOGGER.info("查询到的附件数量是=>"+mediaDatas.size());
            for (MediaData mediaData : mediaDatas) {
                LOGGER.info(mediaData.toString());
                LOGGER.info("--------------------------------------------");
            }
        }
    }

    /**
     * 查询对象的附件by gremlin
     */
    @Test
    public void readMediaDataByDsl() {
        String tid="tid002";
        String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
        List<MediaData> mediaDatas =g.V(graphId).attachments().toList();
        LOGGER.info("读取到的附件------------------------------------");
        if(mediaDatas!=null) {
            LOGGER.info("查询到的附件数量是=>"+mediaDatas.size());
           /* for (MediaData mediaData : mediaDatas) {
                LOGGER.info(mediaData.toString());
                LOGGER.info("--------------------------------------------");
            }*/
        }
    }

    /**
     * 查询对象的附件by gremlin
     */
    @Test
    public void readMediaDataRawByDsl() {
        String tid="tid002";
        String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
        List<MediaDataRaw> mediaDatas =g.V(graphId).attachmentRaws().toList();
        LOGGER.info("读取到的附件------------------------------------");
        if(mediaDatas!=null) {
            LOGGER.info("查询到的附件数量是=>"+mediaDatas.size());
            /*for (MediaDataRaw mediaDataRaw : mediaDatas) {
                LOGGER.info(mediaDataRaw.toString());
                LOGGER.info("--------------------------------------------");
            }*/
        }
    }


    @Test
    public void readMediaDataByDslByKey() {
        String tid="tid001";
        String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
        List<MediaData> mediaDatas =g.V(graphId).attachments("我是附件的key").toList();
        LOGGER.info("读取到的附件------------------------------------");
        if(mediaDatas!=null) {
            LOGGER.info("查询到的附件数量是=>"+mediaDatas.size());
            for (MediaData mediaData : mediaDatas) {
                LOGGER.info(mediaData.toString());
                LOGGER.info("--------------------------------------------");
            }
        }
    }


    /**
     * 删除指定对象的指定附件
     */
    @Test
    public void dropMediaDataByDsl(){
        String tid="tid002";
        String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
        List<MediaData> mediaDatas = g.V(graphId).attachments("我是附件的key").dropExpand().toList();
        g.tx().commit();
    }

    /**
     * 删除指定对象的指定附件
     */
    @Test
    public void dropMediaDataRowByDsl(){
        String tid="tid002";
        String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
        List<MediaDataRaw> mediaDatas = g.V(graphId).attachmentRaws("我是附件的key3").dropExpand().toList();
        g.tx().commit();
    }
}
