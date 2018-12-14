package cn.tarena.high.weblog;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import cn.tarena.weblog.ClearBolt;
import cn.tarena.weblog.HBaseBolt;
import cn.tarena.weblog.PrintBolt;
import storm.kafka.*;

public class HighWeblogTopology {

    public static void main(String[] args) throws Exception {

        Config conf = new Config();

        BrokerHosts hosts = new ZkHosts("hadoop01:2181,hadoop02:2181,hadoop03:2181");

        SpoutConfig sc = new SpoutConfig(hosts, "weblog", "/weblog", "info");

        sc.scheme = new SchemeAsMultiScheme(new StringScheme());

        KafkaSpout spout = new KafkaSpout(sc);
        ClearBolt clearBolt = new ClearBolt();
        PvBolt pvBolt = new PvBolt();
        UvBolt uvBolt = new UvBolt();
        VvBolt vvBolt = new VvBolt();
        NewIpBolt newIpBolt = new NewIpBolt();
        NewCustBolt newCustBolt = new NewCustBolt();

        HBaseBolt hbaseBolt = new HBaseBolt();
        PrintBolt printBolt = new PrintBolt();
        MysqlBolt mysqlBolt = new MysqlBolt();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafkaSpout", spout);
        builder.setBolt("clearBolt", clearBolt).shuffleGrouping("kafkaSpout");
        builder.setBolt("pvBolt", pvBolt).shuffleGrouping("clearBolt");
        builder.setBolt("uvBolt", uvBolt).shuffleGrouping("pvBolt");
        builder.setBolt("vvBolt", vvBolt).shuffleGrouping("uvBolt");
        builder.setBolt("newIpBolt", newIpBolt).shuffleGrouping("vvBolt");
        builder.setBolt("newCustBolt", newCustBolt).shuffleGrouping("newIpBolt");

        builder.setBolt("HBaseBolt", hbaseBolt).shuffleGrouping("newCustBolt");
        builder.setBolt("printBolt", printBolt).globalGrouping("newCustBolt");
        builder.setBolt("mysqlBolt", mysqlBolt).shuffleGrouping("newCustBolt");

        StormTopology topology = builder.createTopology();
        //LocalCluster cluster=new LocalCluster();

        StormSubmitter cluster = new StormSubmitter();
        //noinspection AccessStaticViaInstance
        cluster.submitTopology("highWeblog", conf, topology);
    }
}
