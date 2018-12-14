package cn.tarena.weblog;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.List;
import java.util.Map;

public class PvBolt extends BaseRichBolt {

    private OutputCollector collector;

    private int pv = 0;

    @Override
    public void execute(Tuple input) {
        try {
            pv++;
            //--获取当前tuple中所有值的集合
            List<Object> values = input.getValues();
            values.add(pv);
            collector.emit(input, values);
            collector.ack(input);
        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(input);
        }


    }

    @Override
    public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
        this.collector = collector;

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "urlname", "uvid", "ssid", "sscount", "sstime", "cip", "pv"));

    }

}
