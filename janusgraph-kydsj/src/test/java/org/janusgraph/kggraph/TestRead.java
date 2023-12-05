package org.janusgraph.kggraph;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

public class TestRead {

    @Test
    public void itt() throws IOException {
        String context = FileUtils.readFileToString(new File("D:\\xss\\big_xlsx3.txt"), Charset.forName("UTF-8"));
        System.out.println(context.length());
    }
}
