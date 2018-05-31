package es.batch.process.movies

import java.util
import java.util.Calendar

import com.google.gson.Gson
import es.common.dao.{HDFSDao, MongoDao}
import es.common.pojo.{Genre, Movie, UserMovieViews, Valoration}
import es.common.util.DateUtil
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  */
object BatchMovie{

  class BatchMovieException(s: String) extends Exception(s) {}

  val hdfsPath = "hdfs://localhost:9000"
  val PROCESS_CSV = "CSV"
  val PROCESS_DAILY = "DAILY"
  val PROCESS_RECOMENDATION = "RECOMENDATION"
  var broker :String = "localhost:9092"
  var topicDruid  :String = "valorations-druid-topic"
  var moviesHashMap = new util.HashMap[String,String]()

  /**
    *
    * @param args
    */
  def main(args: Array[String]) {

    //si no tenemos el numero de parametros esperados damos fallo
    if (args.length < 1) {
      val usage = "Usage: BatchMovie processType(CSV,DAILY)"
      println("[ERROR] - " + usage)
      throw new BatchMovieException("Error numero de parametros: " + usage)
    }

    //tipo de procesamiento
    val processType = args(0)

    if (processType.equalsIgnoreCase(PROCESS_CSV)) {
      processCSVBatch()
    } else if (processType.equalsIgnoreCase(PROCESS_DAILY)) {
      //si pasamos fecha procesamos lo del dia que dice, sino lo del dia anterior.
      if(args.length == 2){
        val dateToProcess = args(1)
        processDailyBatch(dateToProcess)
      }else{
        var beforeDay = Calendar.getInstance()
        beforeDay.add(Calendar.DAY_OF_MONTH,-1)//restamos un dia para poner el directorio de ayer.
        val dateToProcess = new DateUtil().dateStringFormat("yyyy/MM/dd", beforeDay.getTime)
        processDailyBatch(dateToProcess)
      }
    } else {
      throw new BatchMovieException("Error process type unknow: " + processType+ "Usage:CSV,DAILY")
    }
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
    * Leemos del directorio CSV, y en el caso de que este relleno, vamos a guardar
    * en el directorio del dia anterior para que luego se pueda procesar junto con lo
    * recibido en el streaming.
    * Una vez acabao, borraremos los ficheros de HDFS.
    */
  def processCSVBatch(): Unit = {

    println("[INFO] - Starting BatchMovie process CSV:")
    println(" ----------------------------------")

    val sconf = new SparkConf()
      .setAppName("BatchMovieCSV")
      .setMaster("local[*]")

    val sc = new SparkContext(sconf)
    val sqlContext = new SQLContext(sc)

    val movieDataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .load(hdfsPath+"/csv/movies.csv")

    //movemos al directorio del dia anterior para que queden en el mismo sitio que lo que viene por streaming.
    var beforeDay = Calendar.getInstance()
    beforeDay.add(Calendar.DAY_OF_MONTH,-1)//restamos un dia para poner el directorio de ayer.

    val pathMovies = "/new/movie/"+new DateUtil().dateStringFormat("yyyy/MM/dd", beforeDay.getTime)
    val hdfsDao = new HDFSDao
    hdfsDao.saveToHdfs(movieDataFrame, pathMovies)

    //@TODO borramos el fichero de peliculas

    println(" End Process Movies csv")
    println(" Start Process Ratings csv")

    val ratingDataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .load(hdfsPath+"/csv/ratings.csv")

    //movemos al directorio del dia anterior para que queden en el mismo sitio que lo que viene por streaming.
    val pathRatings = "/new/valorations/"+new DateUtil().dateStringFormat("yyyy/MM/dd", beforeDay.getTime)
    hdfsDao.saveToHdfs(ratingDataFrame, pathRatings)

    //cogemos cada mensaje y lo mandamos a druid porque seran valoraciones que no hemos recibido por streamming
    if(ratingDataFrame!=null){
      println("Cargamos catalogo de peliculas para Druid")
      loadMovies()
      println("enviamos valoraciones a druid")
      val props = new util.HashMap[String,Object]()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,broker)
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
      ratingDataFrame.foreachPartition{recordPartition =>
        val producer = new KafkaProducer[String,String](props)
        recordPartition.foreach{x =>
          var jsonMessage = createJsonToDruid(x.getString(0),x.getString(1),x.getString(2),x.getString(3))
          println(jsonMessage)
          val message = new ProducerRecord[String,String](topicDruid,null,jsonMessage)
          producer.send(message)
          Thread.sleep(1000)
        }
        producer.close()
      }
    }
    //@TODO borramos el fichero de valoraciones

    println(" End Process Ratings csv")
    //acabamos
    sc.stop
  }

  /**
    *
    * @param str
    * @return
    */
  def parseGenre(str: String): Array[Genre] = {
    val gson = new Gson()
    try{
      if(str!= null && str.length>0){
        return gson.fromJson(str,classOf[Array[Genre]])
      }else{
        return null
      }
    }catch {
      case e:Exception => return null
    }

  }

  /**
    *
    * @param userID
    * @param movieID
    * @param valorationMovie
    * @param date
    * @return
    */
  def createJsonToDruid(userID: String,movieID: String,valorationMovie: String, date: String) : String={

    var titleMovie = movieID
    if(moviesHashMap.get(movieID)!=null)titleMovie = moviesHashMap.get(movieID)
    val valoration = new Valoration(movieID,userID, titleMovie,valorationMovie.toDouble,date)
    val gson = new Gson()
    return gson.toJson(valoration)
  }

  /**
    * Por cada pelicula, buscaremos si esta en la bbdd y actualizamos la valoracion
    * @param rddGroupMovies
    */
  def updateValorationMovies(rddGroupMovies: RDD[(String, Iterable[String])], mongoDao:MongoDao) = {
    if(rddGroupMovies!=null){
      rddGroupMovies.foreachPartition{recordPartition =>
        val conn = mongoDao.initConn()
        val gson = new Gson()
        recordPartition.foreach({rdd=>
          val idMovie = rdd._1
          val valorations = rdd._2
          val numValorations = valorations.size
          var sumValorations = 0.0
          for (x<-valorations){
            sumValorations = sumValorations + x.toDouble
          }
          val movie = mongoDao.findMovieById(conn,idMovie)
          if(movie!=null){
            movie.numValorations = movie.numValorations + numValorations
            movie.sumValoration = sumValorations
            movie.avgValorations = movie.sumValoration.toFloat/movie.numValorations
            //guardamos la pelicula con las valoraciones
            val jsonMovie = gson.toJson(movie)
            mongoDao.updateValorationsMovie(conn, idMovie, jsonMovie)
          }
        })
        mongoDao.closeConnection(conn)
      }


    }
  }

  /**
    * Para cada uno de los usuarios que han votado, actualizamos la lista de peliculas vistas
    * @param rddGroupUsers
    */
  def updateUserMovieViewer(rddGroupUsers: RDD[(String, Iterable[String])], mongoDao:MongoDao) = {
    if(rddGroupUsers!=null){
      rddGroupUsers.foreachPartition{recordPartition=>
        val gson = new Gson()
        val conn = mongoDao.initConn()
        recordPartition.foreach({ rdd =>
          val idUser = rdd._1
          val movies = rdd._2
          val user = mongoDao.findUserById(conn,idUser)
          if(user!=null){
            user.moviesView = user.moviesView ++ movies.toList
            mongoDao.updateUserMovieViewer(conn,user.idUser,gson.toJson(user))
          }else{
            val userMovies = new UserMovieViews(idUser,movies.toArray, null)
            mongoDao.insertUserViews(conn,gson.toJson(userMovies))
          }
        })
        mongoDao.closeConnection(conn)
      }
    }
  }

  /**
    *
    * @param dfValorations
    */
  def saveValorationsRecomender(dfValorations: DataFrame) : Unit = {
    val hdfsDao = new HDFSDao
    var actualDate = Calendar.getInstance()
    var month = actualDate.get(Calendar.MONTH)+1
    var strMonth = ""
    if (month<10)strMonth="0"+month
    else strMonth=month.toString
    var year = actualDate.get(Calendar.YEAR)
    val pathRatings = "/recomender/valorations/"+year+"/"+strMonth
    hdfsDao.saveToHdfs(dfValorations, pathRatings)
  }

  /**
    *
    * @param dateToProcess
    */
  def processDailyBatch(dateToProcess: String) : Unit = {
    println("[INFO] - Starting BatchMovie process daily data:")
    println("[INFO] - dateToProcess:"+dateToProcess)
    println(" ----------------------------------")

    val sconf = new SparkConf()
      .setAppName("BatchMovieDaily")
      .setMaster("local")

    val sc = new SparkContext(sconf)
    val sqlContext = new SQLContext(sc)
    val mongoDao = new MongoDao

    //procesamos las peliculas
    val pathMovies = hdfsPath +"/new/movie/"+dateToProcess
    val jobConfiguration = sc.hadoopConfiguration
    if(FileSystem.get(jobConfiguration).exists(new Path("/new/movie/"+dateToProcess))) {
      val dfMovies = sqlContext.read.load(pathMovies)
      val rddsMovies = dfMovies.rdd
      rddsMovies.foreachPartition { partitionRecords =>
        var conn = mongoDao.initConn()
        val gson = new Gson()
        partitionRecords.foreach { record =>
          var movie: Movie = null
          if (record.length == 1) {
            var splitMovie = record.getString(0).split(";")
            movie = new Movie(splitMovie(5), splitMovie(20), splitMovie(8), splitMovie(9), splitMovie(4),
              splitMovie(6), splitMovie(7), splitMovie(11), splitMovie(14), splitMovie(19), 0, 0, parseGenre(splitMovie(3)))
          } else {
            movie = new Movie(record.getString(5), record.getString(20), record.getString(8), record.getString(9), record.getString(4),
              record.getString(6), record.getString(7), record.getString(11), record.getString(14), record.getString(19), 0, 0, parseGenre(record.getString(3)))
          }
          val jsonMovie = gson.toJson(movie)
          //si no existe la guardamos
          if (mongoDao.findMovieById(conn, movie.idMovie) == null) {
            mongoDao.insertMovie(conn, jsonMovie)
          } else {
            println("pelicula " + movie.idMovie + " ya existe")
          }
        }
        mongoDao.closeConnection(conn)
      }
    }else{
      println("[ERROR] - pathMovies("+pathMovies+") not exist")
    }

    //procesamos las valoraciones
    val pathValorations = hdfsPath +"/new/valorations/"+dateToProcess
    if(FileSystem.get(jobConfiguration).exists(new Path("/new/valorations/"+dateToProcess))) {
      val dfValorations = sqlContext.read.load(pathValorations)
      //guardamos las valoraciones en la carpeta del mes de valoraciones para el recomendador
      saveValorationsRecomender(dfValorations)
      val rddsValorations = dfValorations.rdd
      //para cada pelicula todas sus valoraciones
      val rddGroupMovies = rddsValorations.map ( row => (row.getString(1), row.getString(2))).groupByKey()
      updateValorationMovies(rddGroupMovies,mongoDao)
      //para cada usuario las peliculas que ha visto. Entendemos que si ha valorado es que la ha visto
      val rddGroupUsers = rddsValorations.map ( row => (row.getString(0), row.getString(1)+":"+row.getString(2))).groupByKey()
      updateUserMovieViewer(rddGroupUsers,mongoDao)
    }else{
      println("[ERROR] - pathValorations("+pathValorations+") not exist")
    }
    //acabamos
    sc.stop
  }
}