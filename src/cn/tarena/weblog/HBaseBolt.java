package cn.tarena.weblog;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.tarena.dao.HBaseDao;
import cn.tarena.pojo.FluxInfo;

import java.util.Map;

public class HBaseBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void execute(Tuple input) {
        try {
            String url = input.getStringByField("url");
            String urlname = input.getStringByField("urlname");
            String uvid = input.getStringByField("uvid");
            String ssid = input.getStringByField("ssid");
            String sscount = input.getStringByField("sscount");
            String sstime = input.getStringByField("sstime");
            String cip = input.getStringByField("cip");

            FluxInfo f = new FluxInfo();
            f.setUrl(url);
            f.setUrlname(urlname);
            f.setUvid(uvid);
            f.setSsid(ssid);
            f.setSscount(sscount);
            f.setSstime(sstime);
            f.setCip(cip);

            //--将对象存储到HBase里
            HBaseDao.saveToHBase(f);
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
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
    }

}
