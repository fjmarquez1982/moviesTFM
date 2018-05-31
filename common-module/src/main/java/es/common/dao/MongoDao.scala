package es.common.dao

import com.google.gson.Gson
import com.mongodb.DBObject
import com.mongodb.casbah.MongoConnection
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.util.JSON
import es.common.pojo.{Genre, Movie, UserMovieViews}

import scala.collection.mutable.ListBuffer


/**
  *
  */
class MongoDao extends Serializable{

  var DATABASE_NAME = "movies-tfm"
  var MOVIE_COLLECTION = "movie"
  var USER_COLLECTION = "user.views"

  /**
    *
    * @param conn
    * @param jsonMovie
    * @return
    */
  def insertMovie(conn: MongoConnection, jsonMovie: String) = {
    println("insertando pelicula en mongo:" + jsonMovie)
    val mongoDb = conn(DATABASE_NAME)(MOVIE_COLLECTION)
    val dbObject: DBObject = JSON.parse(jsonMovie).asInstanceOf[DBObject]
    mongoDb.insert(dbObject)
  }

  /**
    *
    * @param conn
    * @param jsonUserViews
    * @return
    */
  def insertUserViews(conn: MongoConnection, jsonUserViews: String)={
    println("insertando user views en mongo:" + jsonUserViews)
    val mongoDb = conn(DATABASE_NAME)(USER_COLLECTION)
    val dbObject: DBObject = JSON.parse(jsonUserViews).asInstanceOf[DBObject]
    mongoDb.insert(dbObject)
  }

  /**
    *
    * @param conn
    * @param id
    * @return
    */
  def findMovieById(conn: MongoConnection, id: String): Movie = {
    println("buscando pelicula con id:" + id)
    val mongoDb = conn(DATABASE_NAME)(MOVIE_COLLECTION)
    val dbObject: DBObject = MongoDBObject("idMovie"-> id)
    val u = mongoDb.findOne(dbObject)
    var movie: Movie = null
    for(x<-u){
      val gson = new Gson()
      movie = gson.fromJson(x.toString,classOf[Movie])
    }
    return movie
  }

  /**
    *
    * @param genres
    * @return
    */
  def generateGenreString(genres: String): String = {
    val gson = new Gson()
    val genresObj = gson.fromJson(genres,classOf[Array[Genre]])
    var genreStr = ""
    for(g<-genresObj){
      genreStr = genreStr + g.name+" "
    }
    return genreStr
  }

  /**
    *
    * @param conn
    * @return
    */
  def findMovies(conn: MongoConnection):List[Movie] = {
    val mongoDb = conn(DATABASE_NAME)(MOVIE_COLLECTION)
    val u = mongoDb.find()
    val listMovies = new ListBuffer[Movie]()
    for(x<-u){
      val gson = new Gson()
      val movie = gson.fromJson(x.toString,classOf[Movie])
      listMovies+=movie
    }
    return listMovies.toList
  }

  /**
    *
    * @param conn
    * @return
    */
  def findMoviesForRecomendation(conn: MongoConnection): List[String] = {
    val mongoDb = conn(DATABASE_NAME)(MOVIE_COLLECTION)
    val u = mongoDb.find()
    val listMovies = new ListBuffer[String]()
    for(x<-u){
      var idMovie = ""
      if (x.get("idMovie")!= null ){
        idMovie = x.get("idMovie").toString
      }
      var titleMovie = ""
      if (x.get("titleMovie")!= null ){
        titleMovie = x.get("titleMovie").toString
      }
      var genresMovie = ""
      if (x.get("genresMovie")!= null ){
        genresMovie = generateGenreString(x.get("genresMovie").toString)
      }
      listMovies+=idMovie+","+titleMovie+","+genresMovie
    }
    return listMovies.toList
  }

  /**
    *
    * @param conn
    * @param id
    * @return
    */
  def findUserById(conn: MongoConnection, id: String): UserMovieViews = {
    println("buscando usuario con id:" + id)
    val mongoDb = conn(DATABASE_NAME)(USER_COLLECTION)
    val dbObject: DBObject = MongoDBObject("idUser"-> id)
    val u = mongoDb.findOne(dbObject)
    var user: UserMovieViews = null
    for(x<-u){
      val gson = new Gson()
      user = gson.fromJson(x.toString,classOf[UserMovieViews])
    }
    return user
  }

  /**
    *
    * @param conn
    * @param idMovie
    * @param jsonMovie
    */
  def updateValorationsMovie(conn: MongoConnection,idMovie: String, jsonMovie: String): Unit = {
    println("actualizamos la pelicula con id:" + idMovie+" con json: "+jsonMovie)
    val mongoDb = conn(DATABASE_NAME)(MOVIE_COLLECTION)
    val query = MongoDBObject("idMovie"->idMovie)
    val update: DBObject = JSON.parse(jsonMovie).asInstanceOf[DBObject]
    val results = mongoDb.update(query,update)
  }

  /**
    *
    * @param conn
    * @param userId
    * @param jsonUser
    */
  def updateUserMovieViewer(conn: MongoConnection,userId: String, jsonUser: String): Unit = {
    println("actualizamos el usuario con id:" + userId)
    val mongoDb = conn(DATABASE_NAME)(USER_COLLECTION)
    val query = MongoDBObject("idUser"->userId)
    val update: DBObject = JSON.parse(jsonUser).asInstanceOf[DBObject]
    val results = mongoDb.update(query,update)
  }

  /**
    *
    * @param conn
    * @return
    */
  def findUsers(conn: MongoConnection): List[UserMovieViews] = {
    println("buscando usuarios")
    val mongoDb = conn(DATABASE_NAME)(USER_COLLECTION)
    val u = mongoDb.find()
    val listUsers = new ListBuffer[UserMovieViews]()
    val gson = new Gson()
    for(x<-u){
      listUsers+= gson.fromJson(x.toString,classOf[UserMovieViews])
    }
    return listUsers.toList
  }

  /**
    *
    * @param conn
    */
  def closeConnection(conn: MongoConnection): Unit = {
    conn.close()
  }

  /**
    *
    * @return
    */
  def initConn():MongoConnection={
    return MongoConnection("localhost",27017)
  }

}
