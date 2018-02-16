package com.oracle.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestFS {

    private FileSystem fs;
    private String url = "hdfs://192.168.28.131:8020/";

    @Before
    public void setUp() throws URISyntaxException, IOException, InterruptedException {
        fs = FileSystem.get(new URI(url), new Configuration(), "myroot");

    }

    @Test
    public void testMkDir() throws IOException {

        fs.mkdirs(new Path("/forCl"));
    }

    @Test
    public void testCreateFile() throws IOException {
        FSDataOutputStream os = fs.create(new Path("/forCl/test.txt"));
        for (int i = 0; i < 3; i++) {
            os.write(("Hello Hadoop" + i).getBytes());
        }
        os.flush();
        os.close();
    }

    @Test
    public void testCatFile() throws IOException {
        FSDataInputStream fsDataInputStream = fs.open(new Path("/forCl/test.txt"));
        byte[] temp = new byte[512];
        int len;
        while ((len = fsDataInputStream.read(temp)) != -1) {
            System.out.println(new String(temp, 0, len));
        }
    }

    @Test
    //换目录
    public void testRename() throws IOException {
        fs.rename(new Path("/forCl/test.txt"), new Path("/forCl/te.txt"));
    }

    @Test
//删除 是否级联删除 如果级联  和该目录有关的所有目录都会删除
    public void remove() throws IOException {
        fs.delete(new Path("/forCl/"), true);
    }

    @Test
//上传 从本地拷
    public void upload() throws IOException {
        fs.copyFromLocalFile(new Path("E:/aaa.txt"), new Path("/forCl/ss.txt"));
    }

    @Test
//下载 拷到本地
    public void downLoad() throws IOException {
        fs.copyToLocalFile(new Path("/forCl/ss.txt"), new Path("E://bbb.txt"));
    }

    @Test
    public void testInfo() throws IOException {
        FsStatus fsStatus = fs.getStatus(new Path("/"));
        System.out.println(fsStatus.getUsed());
        System.out.println(fsStatus.getCapacity());
        System.out.println(fsStatus.getRemaining());
    }

    @Test
    public void testInfoA() throws IOException {
        FileStatus fileStatus = fs.getFileStatus(new Path("/forCl/ss.txt"));
        System.out.println(fileStatus.getAccessTime());
        System.out.println(fileStatus.getReplication());
        System.out.println(fileStatus.getLen());
        System.out.println(fileStatus.getOwner());
        System.out.println(fileStatus.getGroup());
        System.out.println(fileStatus.getBlockSize());
    }

    @Test
    public void testDS() throws IOException {
        DistributedFileSystem ds = (DistributedFileSystem) fs;
        DatanodeInfo[] datanodeInfos = ds.getDataNodeStats();
        for (DatanodeInfo dis : datanodeInfos) {
            System.out.println();
        }
    }

    @After
    public void tearDown() {
        try {
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

