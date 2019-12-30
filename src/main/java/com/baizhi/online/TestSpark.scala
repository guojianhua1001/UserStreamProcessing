package com.baizhi.online


import com.baizhi.entity.{HistoryData}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerConfig

object TestSpark {
  def main(args: Array[String]): Unit = {

    //创建ssc对象
    //val conf = new SparkConf().setAppName("LoginEvaluate").setMaster("spark://SparkOnStandalone:7077")
    val conf = new SparkConf().setAppName("LoginEvaluate").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.checkpoint("hdfs://SparkOnStandalone:9000/checkpoint")
    //创建Kafka Source
    val params = Map(
      (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "SparkOnStandalone:9092"),
      (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer]),
      (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer]),
      (ConsumerConfig.GROUP_ID_CONFIG, "g1") // kafka 同组负载均衡 不同组广播
    )
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent, //位置策略：一致性优先
      ConsumerStrategies.Subscribe[String, String](List("evaluate"), params)
    )

    //对RDD Batch进行处理
    dStream
      .map(rdd => rdd.value())
        .print()

    ssc.start()
    ssc.awaitTermination()

  }
}
