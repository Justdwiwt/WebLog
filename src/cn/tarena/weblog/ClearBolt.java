package cn.tarena.weblog;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class ClearBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void execute(Tuple input) {
        try {
            String info = input.getStringByField("str");
            String[] infos = info.split("\\|");
            String url = infos[0];
            String urlname = infos[1];
            String uvid = infos[13];
            String ssid = infos[14].split("_")[0];
            String sscount = infos[14].split("_")[1];
            String sstime = infos[14].split("_")[2];
            String cip = infos[15];
            //--做锚定
            collector.emit(input,
                    new Values(url, urlname, uvid, ssid, sscount, sstime, cip));

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
        declarer.declare(new Fields("url", "urlname", "uvid", "ssid", "sscount", "sstime", "cip"));

    }

}
