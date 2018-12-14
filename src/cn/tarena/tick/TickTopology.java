package cn.tarena.tick;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import cn.tarena.weblog.PrintBolt;

public class TickTopology {

    public static void main(String[] args) throws Exception {
        Config conf = new Config();


        TimeBolt timeBolt = new TimeBolt();
        BrBolt brBolt = new BrBolt();
        AvgDeepBolt avgDeepBolt = new AvgDeepBolt();
        AvgTimeBolt avgTimeBolt = new AvgTimeBolt();

        PrintBolt printBolt = new PrintBolt();
        TickToMysql tickToMysql = new TickToMysql();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setBolt("timeBolt", timeBolt);
        builder.setBolt("brBolt", brBolt).shuffleGrouping("timeBolt");
        builder.setBolt("avgDeepBolt", avgDeepBolt).shuffleGrouping("brBolt");
        builder.setBolt("avgTimeBolt", avgTimeBolt).shuffleGrouping("avgDeepBolt");

        builder.setBolt("printBolt", printBolt).globalGrouping("avgTimeBolt");
        builder.setBolt("tickToMysql", tickToMysql).shuffleGrouping("avgTimeBolt");

        StormTopology topogology = builder.createTopology();
        //LocalCluster cluster=new LocalCluster();
        StormSubmitter cluster = new StormSubmitter();
        //noinspection AccessStaticViaInstance
        cluster.submitTopology("tickTopology", conf, topogology);
    }
}
