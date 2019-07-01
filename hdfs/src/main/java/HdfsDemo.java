import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

/**
 * @Author: jiajunp
 * @Date: 2019/5/28 15:13
 * @Version 1.0
 */
public class HdfsDemo {
    private static String HDFSUri = "hdfs://hadoop:9000";

    public static FileSystem getFileSystem(){

        //读取配置文件
        Configuration conf = new Configuration();

        //文件系统
        FileSystem fs = null;
        String hdfsUri = HDFSUri;
        if (StringUtils.isBlank(hdfsUri)){
            //返回默认文件系统，如果在hadoop集群下运行，使用此种方法可直接获取默认文件系统；
            try{
                fs = FileSystem.get(conf);
            }catch(IOException e){
                e.printStackTrace();
            }
        }else{
            //返回指定的文件系统，如果在本地测试，需要此种方法获取文件系统；
            try{
                URI uri = new URI(hdfsUri.trim());
                fs = FileSystem.get(uri,conf);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return fs ;
    }


    public static void readFile(String filePath)throws IOException{

        Configuration config = new Configuration();
        String file = HDFSUri+filePath;
        FileSystem fs = FileSystem.get(URI.create(file),config);
        //读取文件
        InputStream is =fs.open(new Path(file));
        //读取文件
        IOUtils.copyBytes(is, System.out, 2048, false); //复制到标准输出流
        fs.close();
    }

    public static void main(String[] args) throws Exception {        //连接fs

        FileSystem fs = getFileSystem();
        System.out.println(fs.getUsed());
        /*//创建路径
        mkdir("/dit2");
        //验证是否存在
        System.out.println(existDir("/dit2",false));
        //上传文件到HDFS
        copyFileToHDFS("E:\\java\\data\\crm.sql","/dit/crm.sql");
        //下载文件到本地
        getFile("/dit/crm.sql","E:\\java\\data\\crm_1.sql");
        // getFile(HDFSFile,localFile);
        //删除文件
        rmdir("/dit2");*/
        //读取文件
        readFile("/dit/crm.sql");
    }
}
