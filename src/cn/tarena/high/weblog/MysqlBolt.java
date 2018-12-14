package cn.tarena.high.weblog;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.tarena.dao.MysqlDao;
import cn.tarena.pojo.Tongji;

import java.sql.Date;
import java.util.Map;

public class MysqlBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void execute(Tuple input) {
        try {
            int pv = input.getIntegerByField("pv");
            int uv = input.getIntegerByField("uv");
            int vv = input.getIntegerByField("vv");
            int newIp = input.getIntegerByField("newip");
            int newCust = input.getIntegerByField("newcust");
            Date time = new Date(Long.parseLong(input.getStringByField("sstime")));

            Tongji t = new Tongji();
            t.setTime(time);
            t.setPv(pv);
            t.setUv(uv);
            t.setVv(vv);
            t.setNewIp(newIp);
            t.setNewCust(newCust);

            MysqlDao.saveToMysql(t);
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
        // TODO Auto-generated method stub

    }

}
