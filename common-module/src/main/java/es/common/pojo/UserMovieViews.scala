package es.common.pojo

/**
  *
  * @param user
  * @param movies
  */
class UserMovieViews(user: String, movies: Array[String], moviesByRecomendator: Array[Movie]) {

  var idUser: String = user
  var moviesView: Array[String]=movies
  var moviesRecomended: Array[Movie] = moviesByRecomendator

}

//case class ValorationUser(movieId: String, rating: String)
