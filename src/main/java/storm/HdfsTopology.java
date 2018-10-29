package storm;

import hdfs.TimesFileNameFormat;
import hdfs.CountStrRotationPolicy;
import hdfs.NewFileAction;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.apache.storm.topology.TopologyBuilder;

public class HdfsTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        //指定任务的spout组件
        builder.setSpout("randomStrSpout", new RandomStrSpout());

        //指定任务的第一个bolt组件
//        builder.setBolt("mywordcountsplit", new WordCountSplitBolt())
//                .shuffleGrouping("mywordcountspout");//随机分组
        //指定任务的第二个bolt组件
//        builder.setBolt("mywordcounttotal", new WordCountTotalBolt())
//                .fieldsGrouping("mywordcountsplit", new Fields("word"));

        /**
         * the bolt insert into hadoop
         **/
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(" ");
        // sync the filesystem after every 1k tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);
//        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1.0f, FileSizeRotationPolicy.Units.KB);
        /** rotate file with Date,every month create a new file
         * format:yyyymm.txt
         */
        FileRotationPolicy rotationPolicy = new CountStrRotationPolicy();
        FileNameFormat fileNameFormat = new TimesFileNameFormat().withPath("/test/");
        RotationAction action = new NewFileAction();
        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl("hdfs://127.0.0.1:9000")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy)
                .addRotationAction(action);
        builder.setBolt("hdfsBolt", bolt, 5).globalGrouping("randomStrSpout");




        //创建任务
        StormTopology job = builder.createTopology();
        Config conf = new Config();


        //任务有两种运行方式：1、本地模式   2、集群模式
        //1、本地模式
        try {
            LocalCluster localcluster = new LocalCluster();
            localcluster.submitTopology("MyWordCount", conf, job);
        }catch (Exception e){
            e.printStackTrace();
        }

        //2、集群模式：用于打包jar，并放到storm运行
//        StormSubmitter.submitTopology(args[0], conf, job);
    }


}