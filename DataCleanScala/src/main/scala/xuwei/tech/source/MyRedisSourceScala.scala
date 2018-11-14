package xuwei.tech.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable

/**
  * Created by xuwei.tech on 2018/11/13.
  */
class MyRedisSourceScala extends SourceFunction[mutable.Map[String,String]]{

  val logger = LoggerFactory.getLogger("MyRedisSourceScala")

  val SLEEP_MILLION = 60000

  var isRunning = true
  var jedis: Jedis = _

  override def run(ctx: SourceContext[mutable.Map[String, String]]) = {
    this.jedis = new Jedis("hadoop110", 6379)
    //隐式转换，把java的hashmap转为scala的map
    import scala.collection.JavaConversions.mapAsScalaMap

    //存储所有国家和大区的对应关系
    var keyValueMap = mutable.Map[String,String]()
    while (isRunning){
      try{
        keyValueMap.clear()
        keyValueMap = jedis.hgetAll("areas")

        for( key <- keyValueMap.keys.toList){
          val value = keyValueMap.get(key).get
          val splits = value.split(",")
          for(split <- splits){
            keyValueMap += (key -> split)
          }
        }

        if(keyValueMap.nonEmpty){
          ctx.collect(keyValueMap)

        }else{
          logger.warn("从redis中获取的数据为空！！！")
        }
        Thread.sleep(SLEEP_MILLION);
      }catch {
        case e: JedisConnectionException => {
          logger.error("redis链接异常，重新获取链接", e.getCause)
          jedis = new Jedis("hadoop110", 6379)
        }
        case e: Exception => {
          logger.error("source 数据源异常", e.getCause)
        }
      }
    }

  }

  override def cancel() = {
    isRunning = false
    if(jedis!=null){
      jedis.close()
    }
  }
}
