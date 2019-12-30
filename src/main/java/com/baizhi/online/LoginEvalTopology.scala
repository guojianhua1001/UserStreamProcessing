package com.baizhi.online

import java.text.SimpleDateFormat
import java.util.Date

import com.baizhi.entity.{EvalReport, EvalState, HistoryData}
import com.baizhi.evaluate.EvaluateChain
import com.baizhi.evaluate.impl.{DeviceReplaceEvaluateHandler, InputFeatureEvaluateHandler, LoginCountEvaluateHandler, LoginHabitEvaluateHandler, RegionLoginEvaluateHandler, SpeedEvaluateHandler, WrongPassEvaluateHandler}
import com.baizhi.update.UpdateChain
import com.baizhi.update.impl.{HistoricalHabitsUpdateHandler, HistoryCitiesUpdateHandler, HistoryDevicesUpdateHandler, HistoryPasswordsUpdateHandler, HistoryVectorsUpdateHandler, LastPointUpdateHandler, LastTimeUpdateHandler}
import com.baizhi.util.LogParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.codehaus.jackson.map.ObjectMapper

/**
 * 登录评估拓扑
 */
object LoginEvalTopology {
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
      //获取日志的文本内容
      .map(rdd => rdd.value()
      )
      //对非所需格式的日志记录进行排除
      .filter(line => {
        LogParser.isLegal(line)
      })
      //转化为 kev - value (key为应用名:用户ID, value为日志文本内容)
      .map(line => {
        val str = LogParser.getKey(line)
        (str, line)
      })
      //根据状态进行日志的处理
      .mapWithState(StateSpec.function((key: String, value: Option[String], state: State[EvalState]) => {
        var report: EvalReport = null;
        var evalState: EvalState = null;
        if (state.exists()) {
          //如果上个状态存在，则从状态中获取评估状态
          evalState = state.get()
        } else {
          //如果状态不存在，则初始化evalState
          evalState = new EvalState()
          evalState.historyData = new HistoryData()
        }
        //如果是登录成功日志，就更新历史数据
        if (LogParser.isLoginSuccess(value.get)) {
          val updateChain = new UpdateChain
          updateChain
            .addHandler(new HistoricalHabitsUpdateHandler)
            .addHandler(new HistoryCitiesUpdateHandler)
            .addHandler(new HistoryDevicesUpdateHandler)
            .addHandler(new HistoryPasswordsUpdateHandler)
            .addHandler(new HistoryVectorsUpdateHandler)
            .addHandler(new LastPointUpdateHandler)
            .addHandler(new LastTimeUpdateHandler)
            .doChain(evalState.historyData, LogParser.parseLoginSuccessData(value.get))
        }

        //如果是评估日志，则根据历史数据生成评估报告
        if (LogParser.isEval(value.get)) {
          /*
            更新历史数据中的尝试登录次数
           */
          var count = 0 //尝试登录次数
          //获取日期
          val loginDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
          //如果上一次登录时间也是当天，则历史尝试登录数+1
          if (loginDate.equals(evalState.dayTime)) {
            count = evalState.historyData.getCurrentDayEvalCounts + 1
            evalState.historyData.setCurrentDayEvalCounts(count)
          } else {
            //如果上一次登陆时间不是今天，则历史登录数重置为1，登录日期重置为今天
            evalState.dayTime = loginDate
            evalState.historyData.setCurrentDayEvalCounts(1)
          }
          /*
            生成评估报告
           */
          report = new EvalReport
          val evaluateChain: EvaluateChain = new EvaluateChain
          evaluateChain
            .addHandler(new DeviceReplaceEvaluateHandler)
            .addHandler(new InputFeatureEvaluateHandler)
            .addHandler(new LoginCountEvaluateHandler)
            .addHandler(new LoginHabitEvaluateHandler)
            .addHandler(new RegionLoginEvaluateHandler)
            .addHandler(new SpeedEvaluateHandler)
            .addHandler(new WrongPassEvaluateHandler)
            .doChain(LogParser.parseEvaluateData(value.get), evalState.historyData, report)
        }
        state.update(evalState)
        report
      }))
      .filter(report => report != null)
      .foreachRDD(rdd => {
        rdd.foreachPartition(vs => {

          vs.foreach(report => {
            val objectMapper = new ObjectMapper()
            val jsonValue = objectMapper.writeValueAsString(report)
            KafkaSinks.saveToKafka("evaluate" + report.getApplicationId, jsonValue)
            val put = new Put((report.getReportTime + report.getLoginSequence).getBytes());
            val hbaseConf = new Configuration
            hbaseConf.set("hbase.zookeeper.quorum", "SparkOnStandalone")
            hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
            // 操作多个列使用空格分隔
            //hbaseConf.set(TableInputFormat.SCAN_COLUMNS, "cf1:applicationId cf1:userId cf1:region cf1:loginSequence cf1:reportTime cf1:login_count cf1:password cf1:login_habit cf1:input_feature cf1:region cf1:device cf1:speed")
            val connection = ConnectionFactory.createConnection(hbaseConf)
            val tableName = TableName.valueOf("baizhi:t_evaluate")
            val table = connection.getTable(tableName)
            put.addColumn("info".getBytes, "applicationId".getBytes, report.getApplicationId.getBytes)
            put.addColumn("info".getBytes, "userId".getBytes, report.getUserId.getBytes)
            put.addColumn("info".getBytes, "region".getBytes, report.getRegion.getBytes)
            put.addColumn("info".getBytes, "loginSequence".getBytes, report.getLoginSequence.getBytes)
            put.addColumn("info".getBytes, "reportTime".getBytes, report.getReportTime.toString.getBytes())
            put.addColumn("info".getBytes, "login_count".getBytes, report.getReportItems.get("login_count").toString.getBytes())
            put.addColumn("info".getBytes, "password".getBytes, report.getReportItems.get("password").toString.getBytes())
            put.addColumn("info".getBytes, "login_habit".getBytes, report.getReportItems.get("login_habit").toString.getBytes())
            put.addColumn("info".getBytes, "input_feature".getBytes, report.getReportItems.get("input_feature").toString.getBytes())
            put.addColumn("info".getBytes, "region".getBytes, report.getReportItems.get("region").toString.getBytes())
            put.addColumn("info".getBytes, "device".getBytes, report.getReportItems.get("device").toString.getBytes())
            put.addColumn("info".getBytes, "speed".getBytes, report.getReportItems.get("speed").toString.getBytes())
            table.put(put)
            table.close()
          })

        })
      })

    ssc.start()
    ssc.awaitTermination()

  }
}

/*{
"applicationId":"APP1",
"userId":"USER1009",
"region":"tianjin",
"loginSequence":"ae5b7a07ef4b48bc863f0f94157ff180",
"reportTime":1577509493000,
"reportItems":
{
"login_count":false,
"password":false,
"login_habit":false,
"input_feature":false,
"region":false,
"device":false,
"speed":false
}
}*/
