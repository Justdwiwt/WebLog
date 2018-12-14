package cn.tarena.tick;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.tarena.dao.MysqlDao;
import cn.tarena.pojo.Tongji2;

import java.sql.Date;
import java.util.Map;

public class TickToMysql extends BaseRichBolt {

    @Override
    public void execute(Tuple input) {
        try {
            Date time = new Date(input.getLongByField("endtime"));
            double br = input.getDoubleByField("br");
            double avgdeep = input.getDoubleByField("avgdeep");
            double avgtime = input.getDoubleByField("avgtime");

            Tongji2 t = new Tongji2();
            t.setTime(time);
            t.setBr(br);
            t.setAvgdeep(avgdeep);
            t.setAvgtime(avgtime);

            MysqlDao.tickToMysql(t);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
        // TODO Auto-generated method stub

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        // TODO Auto-generated method stub

    }

}
