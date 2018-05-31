package es.batch.process.movies

import java.util.Calendar

import com.google.gson.Gson
import es.batch.ml.recomended.UserMovieRecomended
import es.common.dao.MongoDao
import es.common.pojo.Movie
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}

object RecomenderMovie extends App {


  println("[INFO] - Starting RecomenderMovie process:")
  println(" ----------------------------------")

  val sconf = new SparkConf()
    .setAppName("RecomenderMovie")
    .setMaster("local[*]")

  val sc = new SparkContext(sconf)
  val sqlContext = new SQLContext(sc)

  val hdfsPath = "hdfs://localhost:9000"

  sc.setCheckpointDir(hdfsPath+"/checkPointRecomender")

  val jobConfiguration = sc.hadoopConfiguration

  import sqlContext.implicits._

  val mongoDao = new MongoDao
  var conn = mongoDao.initConn()

  //miramos la fecha actual para coger los datos de este mes
  var actualDate = Calendar.getInstance()
  var actualMonth = actualDate.get(Calendar.MONTH)+1
  var strActualMonth = ""
  if (actualMonth<10)strActualMonth="0"+actualMonth
  else strActualMonth=strActualMonth.toString
  var actualYear = actualDate.get(Calendar.YEAR)
  val pathActualValorations = hdfsPath +"/recomender/valorations/"+actualYear+"/"+strActualMonth
  if(FileSystem.get(jobConfiguration).exists(new Path("/recomender/valorations/"+actualYear+"/"+strActualMonth))) {
    //cogemos ficheros del mes actual
    var ratingsDf = sqlContext.read.load(pathActualValorations)

    val ratingsRDD = ratingsDf.map(row => (row.getString(0).toInt, row.getString(1).toInt, row.getString(2).toDouble))
    //ponemos menos resultados para que el proceso funcione
    val ratings = ratingsRDD.filter(row => row._1 < 10000).toDF("userId", "movieId", "rating")
    var moviesRdd = sc.parallelize(mongoDao.findMoviesForRecomendation(conn))

    var moviesSplit = moviesRdd.map(rddRow => rddRow.split(","))
    var movies = moviesSplit.map({ row =>
      var movieId = 0
      var title = "s/t"
      var genre = "s/g"
      try {
        if (row.size == 2) {
          movieId = row(0).toInt
          title = row(1)
        }
        if (row.size == 3) {
          movieId = row(0).toInt
          title = row(1)
          genre = row(2)
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
      (movieId, title, genre)
    }).toDF("movieId", "title", "genres")

    //datos por usuario
    var users = mongoDao.findUsers(conn)
    for(user <- users){
      //saco para un usuario las peliculas que ha visto
      var userId = user.idUser.toInt
      var moviesViewRdd = sc.parallelize(user.moviesView)
      var moviesView = moviesViewRdd.map(rddRow => rddRow.split(":"))
      var normlizedPersonalRatings = moviesView.map(row => (userId,row(0).toInt,row(1).toDouble)).toDF("user", "product","rating")
      UserMovieRecomended.userId = userId
      UserMovieRecomended.movies = movies
      UserMovieRecomended.valorations = ratings
      UserMovieRecomended.personalRatings = normlizedPersonalRatings
      //entreno el modelo con sus valoraciones
      val model = UserMovieRecomended.train(sqlContext)
      //hago la prediccion para el usuario
      val result = UserMovieRecomended.predict(sqlContext, userId, model)
      val dfResults = result.filter(r => r.user == userId)
      //ordenamos por el ratio de recomendacion y cogemos 20
      val recommendationList = dfResults.toDF().sort(col("rating").desc).limit(20)
      //nos quedamos solo con el ID
      val moviesIdRecomended = recommendationList.map(row=>row.getInt(1).toString).collect()
      val listMovies: Array[Movie] = new Array(moviesIdRecomended.length)
      //
      for(count<-moviesIdRecomended.length-1 to 0 by -1) {
        listMovies(moviesIdRecomended.length-1-count)= mongoDao.findMovieById(conn,moviesIdRecomended(count))
      }
      //
      user.moviesRecomended = listMovies
      val gson = new Gson()
      mongoDao.updateUserMovieViewer(conn,user.idUser,gson.toJson(user))
    }
  } else {
    println("no data to generate movie recomendations")
  }
  //guardamos la lista de recomendaciones
  mongoDao.closeConnection(conn)
}
