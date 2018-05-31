package es.common.pojo

/**
  *
  * @param idGenre
  * @param genre
  */
class Genre(idGenre: String,
            genre:String) extends Serializable{

  var id = idGenre
  var name = genre
}
