package es.common.pojo

/**
  *
  * @param identifier
  * @param title
  * @param originalTitle
  * @param overview
  * @param homepage
  * @param imdbId
  * @param originalLanguage
  * @param posterPath
  * @param releaseDate
  * @param tagline
  * @param voteAverage
  * @param voteCount
  */
class Movie (identifier : String,
             title : String,
             originalTitle : String,
             overview : String,
             homepage : String ,
             imdbId: String,
             originalLanguage: String,
             posterPath: String,
             releaseDate: String,
             tagline: String,
             voteAverage: Double,
             voteCount: Integer,
             genres: Array[Genre]
            ) extends Serializable{
  var idMovie = identifier
  var titleMovie = title
  var originalTitleMovie = originalTitle
  var overviewMovie = overview
  var homepageMovie = homepage
  var imdbIdMovie = imdbId
  var originalLanguageMovie = originalLanguage
  var posterPathMovie = posterPath
  var releaseDateMovie = releaseDate
  var taglineMovie = tagline
  var sumValoration = voteAverage
  var numValorations = voteCount
  var genresMovie = genres
  var avgValorations: Double = 0
  /**
    *
    */
  def printMovie = println("id:"+idMovie+" title:"+titleMovie)


}