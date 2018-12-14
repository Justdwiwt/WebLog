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

public class NewIpBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void execute(Tuple input) {
        try {
            //--1.获取当前用户的ip地址
            //--2.通过HBase的列值过滤器，去weblog表查询所有数据（含当天的）
            //--3.如果查询返回的结果集List<FluxInfo>,
            //--有数据，则说明不是一个新增的ip,则记newip=0,反之记为1
            String cip = input.getStringByField("cip");
            String familyName = "cf1";
            String colName = "cip";
            //--匹配 列族 列名 列值
            List<FluxInfo> results = HBaseDao.queryByColumn(
                    familyName, colName, cip);
            int newIp = results.size() == 0 ? 1 : 0;
            List<Object> values = input.getValues();
            values.add(newIp);
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
        declarer.declare(new Fields("url", "urlname", "uvid", "ssid", "sscount", "sstime", "cip", "pv", "uv", "vv", "newip"));

    }

}
