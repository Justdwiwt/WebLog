package cn.tarena.high.weblog;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import cn.tarena.dao.HBaseDao;
import cn.tarena.pojo.FluxInfo;

import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class UvBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void execute(Tuple input) {
        try {
            //--当一个用户发现访问时，我们会接受到此用户的uvid
            //--1:拿到uvid,去HBase的weblog表去查询，查当天的数据
            //--如果此uvid没有出现过，说明是新用户，则记uv=1,反之记uv=0
            //--2:难点在于如何根据用户的访问记录定位当天这个语义
            //--去HBase表查询时，扫描范围:startTime=当天的0:0 endTime=sstime

            //--获取用户访问的时间戳
            String endtime = input.getStringByField("sstime");
            Calendar calendar = Calendar.getInstance();
            //--下面代码表示以endtime为基准，找当天的0:0:0:0
            calendar.setTimeInMillis(Long.parseLong(endtime));
            calendar.set(Calendar.HOUR, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            //--获取当天的0:0:0:0所对应的时间戳
            String startTime = String.valueOf(calendar.getTimeInMillis());

            //--3:把扫描范围定好之后，接下来要考虑如何通过当前的uvid去HBase查询是否出现过
            //--行键:sstime_uvid_ssid_两位随机数
            //--根据以上思路，可以通过HBase的行键过滤器来匹配符合条件的行数据
            String uvid = input.getStringByField("uvid");
            //--正则表达式:^\d{13}_uvid_\d{10}_\d{2}$
            String regex = "^\\d{13}_" + uvid + "_\\d{10}_\\d{2}$";
            List<FluxInfo> results = HBaseDao.queryByRange(startTime, endtime, regex);
            //--如果结果集为0,说明当前的uvid没有在HBase出现过，是新用户
            int uv = results.size() == 0 ? 1 : 0;

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
