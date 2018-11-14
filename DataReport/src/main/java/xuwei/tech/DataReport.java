package xuwei.tech;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.OutputTag;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xuwei.tech.function.MyAggFunction;
import xuwei.tech.watermark.MyWatermark;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 *
 * bin/kafka-topics.sh  --create --topic lateLog --zookeeper localhost:2181 --partitions 5 --replication-factor 1
 *
 * Created by xuwei.tech on 2018/11/14.
 */
public class DataReport {
    private static Logger logger = LoggerFactory.getLogger(DataReport.class);

    public static void main(String[] args) throws Exception{


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(5);

        //设置使用eventtime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //checkpoint配置
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置statebackend

        //env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop100:9000/flink/checkpoints",true));

        /**
         * 配置kafkaSource
         */
        String topic = "auditLog";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","hadoop110:9092");
        prop.setProperty("group.id","con1");

        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop);


        /**
         * 获取到kafka中的数据
         *
         * 审核数据格式
         * // {"dt":"审核时间[年月日 时分秒]","type":"审核类型","username":"审核人员姓名","area":"大区"}
         *
         */
        DataStreamSource<String> data = env.addSource(myConsumer);


        /**
         * 对数据进行清洗
         */
        DataStream<Tuple3<Long, String, String>> mapData = data.map(new MapFunction<String, Tuple3<Long, String, String>>() {
            @Override
            public Tuple3<Long, String, String> map(String line) throws Exception {
                JSONObject jsonObject = JSON.parseObject(line);

                String dt = jsonObject.getString("dt");
                long time = 0;
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date parse = sdf.parse(dt);
                    time = parse.getTime();
                } catch (ParseException e) {
                    //也可以把这个日志存储到其他介质中
                    logger.error("时间解析异常，dt:" + dt, e.getCause());
                }

                String type = jsonObject.getString("type");
                String area = jsonObject.getString("area");

                return new Tuple3<>(time, type, area);
            }
        });

        /**
         * 过滤掉异常数据
         */
        DataStream<Tuple3<Long, String, String>> filterData = mapData.filter(new FilterFunction<Tuple3<Long, String, String>>() {
            @Override
            public boolean filter(Tuple3<Long, String, String> value) throws Exception {
                boolean flag = true;
                if (value.f0 == 0) {
                    flag = false;
                }
                return flag;
            }
        });

        //保存迟到太久的数据
        OutputTag<Tuple3<Long, String, String>> outputTag = new OutputTag<Tuple3<Long, String, String>>("late-data"){};

        /**
         * 窗口统计操作
         */
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> resultData = filterData.assignTimestampsAndWatermarks(new MyWatermark())
                .keyBy(1, 2)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .allowedLateness(Time.seconds(30))//允许迟到30s
                .sideOutputLateData(outputTag)//记录迟到太久的数据
                .apply(new MyAggFunction());


        //获取迟到太久的数据
        DataStream<Tuple3<Long, String, String>> sideOutput = resultData.getSideOutput(outputTag);

        //把迟到的数据存储到kafka中
        String outTopic = "lateLog";
        Properties outprop = new Properties();
        outprop.setProperty("bootstrap.servers","hadoop110:9092");
        outprop.setProperty("transaction.timeout.ms",60000*15+"");

        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(outTopic, new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()), outprop, FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);

        sideOutput.map(new MapFunction<Tuple3<Long,String,String>, String>() {
            @Override
            public String map(Tuple3<Long, String, String> value) throws Exception {
                return value.f0+"\t"+value.f1+"\t"+value.f2;
            }
        }).addSink(myProducer);


        /**
         * 把计算的结果存储到es中
         */
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop100", 9200, "http"));

        ElasticsearchSink.Builder<Tuple4<String, String, String, Long>> esSinkBuilder = new ElasticsearchSink.Builder<Tuple4<String, String, String, Long>>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple4<String, String, String, Long>>() {
                    public IndexRequest createIndexRequest(Tuple4<String, String, String, Long> element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("time",element.f0);
                        json.put("type",element.f1);
                        json.put("area",element.f2);
                        json.put("count",element.f3);

                        //使用time+type+area 保证id唯一
                        String id = element.f0.replace(" ","_")+"-"+element.f1+"-"+element.f2;

                        return Requests.indexRequest()
                                .index("auditindex")
                                .type("audittype")
                                .id(id)
                                .source(json);
                    }

                    @Override
                    public void process(Tuple4<String, String, String, Long> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        //设置批量写数据的缓冲区大小，实际工作中的时间这个值需要调大一些
        esSinkBuilder.setBulkFlushMaxActions(1);
        resultData.addSink(esSinkBuilder.build());


        env.execute("DataReport");


    }
}
