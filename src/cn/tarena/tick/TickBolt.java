package cn.tarena.tick;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

@SuppressWarnings("unused")
public class TickBolt extends BaseRichBolt {

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        //--此参数表示每隔10s,触发一次execute()方法
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);

        return conf;
    }

    @Override
    public void execute(Tuple arg0) {
        System.out.println("hello1806");

    }

    @Override
    public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
    }

}
