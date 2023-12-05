package org.janusgraph.kggraph;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestHbase {
    @Test
    public void splitKeys(){
        int numRegions=30;
        String minRowKeyPrefix="00000000";
        String maxRowKeyPrefix="ffffffff";
        long minInt=Long.parseLong(minRowKeyPrefix,16);
        long maxInt=Long.parseLong(maxRowKeyPrefix,16);
        long range=maxInt-minInt;
        int prepareRegions=numRegions;
        int splitKeysNumber = prepareRegions - 1;
        long splitKeysBase = range / prepareRegions;
        byte[][] splitKeys = new byte[(int)splitKeysNumber][];
        for(int i = 1; i < prepareRegions ; i ++) {
            Long endNumber=(i*splitKeysBase);
            String endRow=endNumber.toHexString(endNumber);
            /*if(i==prepareRegions-1)
            {
                endRow=maxRowKeyPrefix;
            }*/
            int w=maxRowKeyPrefix.length()-endRow.length();
            StringBuffer endRowKey=new StringBuffer();
            for(int j=0;j<w;j++)
            {
                endRowKey.append("0");
            }
            endRowKey.append(endRow);
            System.out.println(endRowKey.toString());
            splitKeys[i-1] = Bytes.toBytes(endRowKey.toString());
        }
        System.out.println(splitKeys);
    }
}
