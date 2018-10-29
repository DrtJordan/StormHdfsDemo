package hdfs;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
/**
    当转换写入文件时候调用的 hook ，这里仅写入日志。
 */
public class NewFileAction implements RotationAction {
    private static final Logger LOG = LoggerFactory.getLogger(NewFileAction.class);



    @Override
    public void execute(FileSystem fileSystem, Path filePath) throws IOException {
        LOG.info("Hdfs change the written file！！");

        return;
    }
}
