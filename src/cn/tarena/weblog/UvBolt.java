package cn.tarena.weblog;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("unchecked")
public class UvBolt extends BaseRichBolt {

    private OutputCollector collector;
    private Set<String> uvSet = new HashSet();

    @Override
    public void execute(Tuple input) {
        try {
            String uvid = input.getStringByField("uvid");
            uvSet.add(uvid);
            //--统计独立用户数，即统计一共有多少个不同的uvid
            int uv = uvSet.size();
            List<Object> values = input.getValues();
            values.add(uv);
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
        declarer.declare(new Fields("url", "urlname", "uvid", "ssid", "sscount", "sstime", "cip", "pv", "uv"));

    }

}
