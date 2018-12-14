package cn.tarena.weblog;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Map;

public class PrintBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void execute(Tuple input) {
        try {
            //--获取上游tuple中所有的key字段
            Fields keys = input.getFields();
            //--获取key的迭代器
            for (String key : keys) {
                //--获取一个key
                Object value = input.getValueByField(key);
                System.out.println(key + ":" + value);
            }
            collector.ack(input);
        } catch (Exception e) {
            collector.fail(input);
        }


    }

    @Override
    public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
        this.collector = collector;

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        // TODO Auto-generated method stub

    }

}
