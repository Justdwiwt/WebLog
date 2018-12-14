package cn.tarena.tick;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import cn.tarena.dao.HBaseDao;
import cn.tarena.pojo.FluxInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvgTimeBolt extends BaseRichBolt {
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

            Map<String, List<String>> ssMap = new HashMap<>();

            for (FluxInfo result : results) {
                String ssid = result.getSsid();
                String sstime = result.getSstime();

                if (ssMap.containsKey(ssid)) {

                    ssMap.get(ssid).add(sstime);
                } else {
                    //--存储当前会话的所有时间戳的list
                    List<String> sstimeList = new ArrayList<>();
                    sstimeList.add(sstime);
                    ssMap.put(ssid, sstimeList);
                }
            }
            int vv = ssMap.size();
            long totalTime = 0;

            for (Map.Entry<String, List<String>> entry : ssMap.entrySet()) {

                List<String> sstimeList = entry.getValue();
                //--接下来，遍历sstimeList,找出当前会话的最大时间戳和最小时间戳
                long maxTime = Long.MIN_VALUE;
                long minTime = Long.MAX_VALUE;
                for (String sstime : sstimeList) {

                    if (maxTime < Long.parseLong(sstime)) {
                        maxTime = Long.parseLong(sstime);
                    }
                    if (minTime > Long.parseLong(sstime)) {
                        minTime = Long.parseLong(sstime);
                    }
                }
                //--将总的会话时长累加
                totalTime = totalTime + (maxTime - minTime);
            }
            //--平均的会话时长
            double avgTime = 0.0;
            if (vv != 0) {
                avgTime = (totalTime * 1.0) / (vv * 1.0);
            }

            List<Object> values = input.getValues();
            values.add(avgTime);
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
        declarer.declare(new Fields("endtime", "br", "avgdeep", "avgtime"));

    }

}
