package es.streaming.kafka

import java.util
import java.util.Calendar

import com.google.gson.Gson
import es.common.dao.{HDFSDao, MongoDao}
import es.common.pojo.Valoration
import es.common.util.DateUtil
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  */
object StreamingKafkaConsumer {


  var broker :String = "localhost:9092"
  var topicDruid  :String = "valorations-druid-topic"
  var topicMovies  :String = "movies-topic"
  var topicValorations  :String= "valorations-topic"
  var moviesHashMap = new util.HashMap[String,String]()

  /**
    *
    * @param rdd
    */
  def processMovie(rdd: RDD[(String)]): Unit = {
    if(rdd.count() > 0){
      //guardamos en HDFS para que luego batch lo procese
      var path = "/new/movie"
      path = path +"/"+new DateUtil().dateStringFormat("yyyy/MM/dd", Calendar.getInstance().getTime)
      val hdfsDao = new HDFSDao
      //cambiamos a dataFrame
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._
      val movieDataFrame = rdd.toDF()
      hdfsDao.saveToHdfs(movieDataFrame, path)
    }
  }

  /**
    *
    * @param x
    * @return
    */
  def createJsonToDruid(x: String) : String={
    val splitVal = x.split(",")
    var titleMovie = splitVal(1)
    if(moviesHashMap.get(splitVal(1))!=null)titleMovie = moviesHashMap.get(splitVal(1))
    val valoration = new Valoration(splitVal(1),splitVal(0), titleMovie,splitVal(2).toDouble,splitVal(3))
    val gson = new Gson()
    return gson.toJson(valoration)
  }

  /**
    *
    * @param rdd
    */
  def sendMessageDruid(rdd: RDD[String]): Unit = {
    val props = new util.HashMap[String,Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,broker)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    rdd.foreach{x =>
      var jsonMessage = createJsonToDruid(x)
      val producer = new KafkaProducer[String,String](props)
      val message = new ProducerRecord[String,String](topicDruid,null,jsonMessage)
      producer.send(message)
      producer.close()
    }
  }

  /**
    *
    * @param rdd
    */
  def processValoration(rdd: RDD[(String)]): Unit = {
    if(rdd.count() > 0) {
      //guardamos en HDFS para que luego batch lo procese
      var path = "/new/valorations"
      path = path +"/"+new DateUtil().dateStringFormat("yyyy/MM/dd", Calendar.getInstance().getTime)
      //cambiamos a dataFrame
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._
      val valorationDataFrame = rdd.toDF()
      val hdfsDao = new HDFSDao
      hdfsDao.saveToHdfs(valorationDataFrame, path)
      //guardamos en druid
      sendMessageDruid(rdd)
    }
  }

  /**
    *
    * @param sc
    */
  def loadProperties(sc: SparkContext):Unit = {
    broker = sc.getConf.get("broker")
    topicDruid = sc.getConf.get("topicDruid")
    topicMovies = sc.getConf.get("topicMovies")
    topicValorations = sc.getConf.get("topicValorations")
  }

  /**
    * cargamos las peliculas en memoria para poder usarlas luego
    */
  def loadMovies():Unit = {
    var mongoDao = new MongoDao
    var con = mongoDao.initConn()
    var listMovies = mongoDao.findMovies(con)
    if(listMovies!=null && listMovies.size>0){
      for(movie<-listMovies){
        if(moviesHashMap.get(movie.idMovie) == null){
          moviesHashMap.put(movie.idMovie,movie.titleMovie)
        }
      }
    }
  }

  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingKafka")
    val sc = new SparkContext(conf)
    val scc = new StreamingContext(sc,Seconds(10))
    //
    //loadProperties(sc)
    //
    loadMovies()
    //
    //consumidor de kafka
    val kafkaParams = Map[String, String](
        "metadata.broker.list" -> broker,
        "enable.auto.commit" -> String.valueOf(true)
    )
    val messagesMovies = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc, kafkaParams, Set(topicMovies)).map(_._2)
    val messagesValorations = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc, kafkaParams, Set(topicValorations)).map(_._2)

    //procesamos las peliculas
    messagesMovies.foreachRDD { rddMovie =>
      processMovie(rddMovie)
    }
    //procesamos las valoraciones
    messagesValorations.foreachRDD { rddValoration =>
      processValoration(rddValoration)
    }
    //
    scc.start()
    scc.awaitTermination()
  }
}
