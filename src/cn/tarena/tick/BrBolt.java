package cn.tarena.tick;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import cn.tarena.dao.HBaseDao;
import cn.tarena.pojo.FluxInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BrBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void execute(Tuple input) {
        try {
            long endtime = input.getLongByField("endtime");
            //--以endtime为基准，获取6小时前的时间戳
            long startTime = endtime - 1000 * 60 * 60 * 60;
            //--匹配所有数据
            String regex = "^.*$";

            //--返回6小时之内所有的行数据对象
            List<FluxInfo> results = HBaseDao.queryByRange(
                    String.valueOf(startTime),
                    String.valueOf(endtime), regex);

            //--接下来统计BR页面率
            //--BR=跳出会话数/总的会话数
            //--key=ssid value=此会话产生的访问记录数
            Map<String, Integer> ssMap = new HashMap<>();

            for (FluxInfo result : results) {
                String ssid = result.getSsid();
                if (ssMap.containsKey(ssid)) {
                    //--如果此会话id已经出现过，则会话次数+1
                    ssMap.put(ssid, ssMap.get(ssid) + 1);
                } else {
                    ssMap.put(ssid, 1);
                }
            }
            //--获取总的会话数
            int vv = ssMap.size();

            int brCount = 0;
            for (Map.Entry<String, Integer> entry : ssMap.entrySet()) {
                if (entry.getValue() == 1) {
                    //--将跳出会话数做叠加
                    brCount++;
                }
            }

            double br = 0.0;
            if (vv != 0) {
                br = (brCount * 1.0) / (vv * 1.0);
            }

            List<Object> values = input.getValues();
            values.add(br);
            collector.emit(values);

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    @Override
    public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
        this.collector = collector;

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("endtime", "br"));

    }

}
