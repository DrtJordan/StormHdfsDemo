package storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

//采集数据: Spout组件
public class RandomStrSpout extends BaseRichSpout {
    //模拟产生一些数据
    private String[] data = {"I love Beijing","I love China","Beijing is the capital of China"};

    //collector：该Spout组件的收集器，用于把采集的数据发给下一个组件
    private SpoutOutputCollector collector;

    @Override
    public void nextTuple() {
        //每隔1毫秒 采集一次数据
        Utils.sleep(100);
        //由Storm的引擎调用，用于处理采集的每条数据

        //生成一个3以内的随机数
        int random = (new Random()).nextInt(3);
        String value = data[random];
        //打印
        System.out.println("采集的数据是：" + value);

        //发送给下一个组件
        this.collector.emit(new Values(value));
    }

    @Override
    public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
        //collector：该Spout组件的收集器，用于把采集的数据发给下一个组件
        //在open方法中对collector初始化
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declare) {
        // 申明发送给下一个组建的tuple的schema（结构）
        declare.declare(new Fields("sentence"));
    }

    /*
   */
}
