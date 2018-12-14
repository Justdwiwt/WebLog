package cn.tarena.tick;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import cn.tarena.dao.HBaseDao;
import cn.tarena.pojo.FluxInfo;

import java.util.*;

public class AvgDeepBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void execute(Tuple input) {
        try {
            long endtime = input.getLongByField("endtime");
            long starttime = endtime - 1000 * 60 * 60 * 6;
            String regex = "^.*$";

            List<FluxInfo> results = HBaseDao.queryByRange(
                    String.valueOf(starttime),
                    String.valueOf(endtime),
                    regex);

            Map<String, Set<String>> ssMap = new HashMap<>();

            for (FluxInfo result : results) {
                String ssid = result.getSsid();
                if (ssMap.containsKey(ssid)) {

                    ssMap.get(ssid).add(result.getUrl());

                } else {
                    String url = result.getUrl();
                    Set<String> urlSet = new HashSet<>();
                    urlSet.add(url);

                    ssMap.put(ssid, urlSet);
                }
            }
            int vv = ssMap.size();
            int totalDeep = 0;
            for (Map.Entry<String, Set<String>> entry : ssMap.entrySet()) {
                //--将每个会话深度累加求和
                totalDeep = totalDeep + entry.getValue().size();
            }

            double avgdeep = 0.0;
            if (vv != 0) {
                //--平均的会话深度
                avgdeep = (totalDeep * 1.0) / (vv * 1.0);
            }
            List<Object> values = input.getValues();
            values.add(avgdeep);
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
        declarer.declare(new Fields("endtime", "br", "avgdeep"));

    }

}
