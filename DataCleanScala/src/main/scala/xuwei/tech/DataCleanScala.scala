package xuwei.tech

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper
import org.apache.flink.util.Collector
import xuwei.tech.source.MyRedisSourceScala

import scala.collection.mutable

/**
  * Created by xuwei.tech on 2018/11/12.
  */
object DataCleanScala {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //修改并行度
    env.setParallelism(5)

    //checkpoint配置
    env.enableCheckpointing(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //设置statebackend

    //env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop100:9000/flink/checkpoints",true))


    //隐式转换
    import org.apache.flink.api.scala._
    val topic = "allData"
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","hadoop110:9092")
    prop.setProperty("group.id","consumer2")

    val myConsumer = new FlinkKafkaConsumer011[String]("hello",new SimpleStringSchema(),prop)
    //获取kafka中的数据
    val data = env.addSource(myConsumer)

    //最新的国家码和大区的映射关系
    val mapData = env.addSource(new MyRedisSourceScala).broadcast //可以吧数据发送到后面算子的所有并行实例中

    val resData = data.connect(mapData).flatMap(new CoFlatMapFunction[String, mutable.Map[String, String], String] {

      //存储国家和大区的映射关系
      var allMap = mutable.Map[String,String]()

      override def flatMap1(value: String, out: Collector[String]): Unit = {

        val jsonObject = JSON.parseObject(value)
        val dt = jsonObject.getString("dt")
        val countryCode = jsonObject.getString("countryCode")
        //获取大区
        val area = allMap.get(countryCode).getOrElse("other")

        val jsonArray = jsonObject.getJSONArray("data")
        for (i <- 0 to jsonArray.size()-1) {
          val jsonObject1 = jsonArray.getJSONObject(i)
          jsonObject1.put("area", area)
          jsonObject1.put("dt", dt)
          out.collect(jsonObject1.toString)
        }
      }

      override def flatMap2(value: mutable.Map[String, String], out: Collector[String]): Unit = {
        this.allMap = value
      }
    })


    val outTopic = "allDataClean"
    val outprop = new Properties()
    outprop.setProperty("bootstrap.servers","hadoop110:9092")
    //第一种解决方案，设置FlinkKafkaProducer011里面的事务超时时间
    //设置事务超时时间
    //prop.setProperty("transaction.timeout.ms",60000*15+"")

    //第二种解决方案，设置kafka的最大事务超时时间

    val myProducer = new FlinkKafkaProducer011[String](outTopic, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema), outprop, FlinkKafkaProducer011.Semantic.EXACTLY_ONCE)
    resData.addSink(myProducer)


    env.execute("DataCleanScala")




  }

}
