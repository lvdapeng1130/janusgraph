package org.janusgraph.kydsj;

import java.util.UUID;

/**
 * @author: ldp
 * @time: 2020/8/7 10:35
 * @jira:
 */
public class Test {
    public static void main(String[] args) {
        for(int i=0;i<100;i++) {
            long leastSignificantBits = UUID.randomUUID().getMostSignificantBits();
            System.out.println(leastSignificantBits);
        }
    }
}
