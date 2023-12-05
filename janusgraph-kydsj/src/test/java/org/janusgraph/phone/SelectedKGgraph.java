package org.janusgraph.phone;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * @author: ldp
 * @time: 2021/1/18 15:27
 * @jira:
 */
public class SelectedKGgraph extends AbstractKGgraphTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SelectedKGgraph.class);

    /**
     * 根据tid查询顶点对象
     */
    @Test
    public void selectByTid1(){
        //String tid="tid0014";
        Optional<Long> call = g.V().hasLabel("Call").count().tryNext();
        System.out.println(call.get());
    }
}
