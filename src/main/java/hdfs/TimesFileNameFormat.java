package hdfs;

import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.task.TopologyContext;

import java.util.Map;

/**
 * 决定重新写入文件时候的名字
 * 这里会返回是第几次转换写入文件，将这个第几次做为文件名
 */
public class TimesFileNameFormat implements FileNameFormat {
    //默认路径
    private String path = "/storm";
    //默认后缀
    private String extension = ".txt";
    private Long times = new Long(0);

    public TimesFileNameFormat withPath(String path){
        this.path = path;
        return this;
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext) {
    }


    @Override
    public String getName(long rotation, long timeStamp) {
        times ++ ;
        //返回文件名，文件名为更换写入文件次数
        return times.toString() + this.extension;
    }

    public String getPath(){
        return this.path;
    }
}
