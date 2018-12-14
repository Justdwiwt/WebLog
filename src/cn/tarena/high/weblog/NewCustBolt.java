package cn.tarena.high.weblog;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import cn.tarena.dao.HBaseDao;
import cn.tarena.pojo.FluxInfo;

import java.util.List;
import java.util.Map;

public class NewCustBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void execute(Tuple input) {
        try {
            String uvid = input.getStringByField("uvid");
            List<FluxInfo> results = HBaseDao.queryByColumn("cf1", "uvid", uvid);
            int newcust = results.size() == 0 ? 1 : 0;
            List<Object> values = input.getValues();
            values.add(newcust);
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
        declarer.declare(new Fields("url", "urlname", "uvid", "ssid", "sscount", "sstime", "cip", "pv", "uv", "vv", "newip", "newcust"));

    }

}
