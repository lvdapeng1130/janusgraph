package org.janusgraph.diskstorage.hbase;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.janusgraph.kydsj.ContentStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HDFSManager {
    private static final Logger logger = LoggerFactory.getLogger(HDFSManager.class);
    public static final String ROOT_DIRECTORY_NAME="/trsgraph";
    public static Path openGraphHDFS(FileSystem fs,String graphName) throws IOException {
        return HDFSManager.createDir(fs,graphName);
    }
    public static Path createDir(FileSystem fs,String graphName) throws IOException {
        Path directoryPath = new Path(ROOT_DIRECTORY_NAME);
        Path graphPath=new Path(directoryPath,graphName);
        if (!fs.exists(graphPath)) {
            boolean success = fs.mkdirs(graphPath);
            if (success) {
                logger.info(String.format("在hdfs上创建目录%s成功！",graphPath));
            } else {
                logger.warn(String.format("在hdfs上创建目录%s失败！",graphPath));
            }
        }
        return graphPath;
    }

    /**
     * 把文件上传到hdfs
     * @param fs
     * @param directory
     * @param fileName
     * @param context
     * @throws IOException
     */
    public static void uploadHdfs(FileSystem fs,Path directory,String fileName,byte[] context) throws IOException {
        Path filePath = new Path(directory, fileName);
        try(FSDataOutputStream outputStream = fs.create(filePath, true);) {
            outputStream.write(context);
        }
    }

    /**
     * 读取hdfs具体文件
     * @param fs
     * @param directory
     * @param fileName
     * @return
     * @throws IOException
     */
    public static byte[] downloadHdfs(FileSystem fs,Path directory,String fileName) throws IOException {
        Path filePath = new Path(directory,fileName);
        if (fs.exists(filePath)) {
            try (FSDataInputStream inputStream = fs.open(filePath)) {
                byte[] bytes = IOUtils.toByteArray(inputStream);
                return bytes;
            }
        }
        return null;
    }

    /**
     * 删除hdfs目录下具体文件
     * @param fs
     * @param directory
     * @param fileName
     * @throws IOException
     */
    public static void deleteHdfs(FileSystem fs,Path directory,String fileName) throws IOException {
        Path filePath = new Path(directory,fileName);
        if (fs.exists(filePath)) {
            fs.delete(filePath,false);
        }
    }

    /**
     * 删除hdfs目录
     * @param fs
     * @param directory
     * @throws IOException
     */
    public static void deleteDirectory(FileSystem fs,Path directory) throws IOException {
        if (fs.exists(directory)) {
            fs.delete(directory,true);
        }
    }

    public static ContentStatus getContentStatus(FileSystem fs, Path directory, String fileName) throws IOException {
        Path filePath = new Path(directory,fileName);
        if (fs.exists(filePath)) {
            FileStatus fileStatus = fs.getFileStatus(filePath);
            ContentStatus contentStatus=new ContentStatus();
            if(fileStatus.getPath() != null){
                contentStatus.setHdfsPath(fileStatus.getPath().toString());
            }
            contentStatus.setPath(filePath.toString());
            contentStatus.setName(filePath.getName());
            contentStatus.setLength(fileStatus.getLen());
            contentStatus.setOwner(fileStatus.getOwner());
            contentStatus.setAccess_time(fileStatus.getAccessTime());
            contentStatus.setModification_time(fileStatus.getModificationTime());
            contentStatus.setBlocksize(fileStatus.getBlockSize());
            contentStatus.setIsdir(fileStatus.isDirectory());
            contentStatus.setGroup(fileStatus.getGroup());
            contentStatus.setBlock_replication(fileStatus.getReplication());
            return contentStatus;
        }
        return null;
    }
}
