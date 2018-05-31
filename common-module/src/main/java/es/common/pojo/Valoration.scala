package es.common.pojo

/**
  *
  * @param identifierMovie
  * @param identifierUser
  * @param title
  * @param valorationMovie
  * @param time
  */
class Valoration(identifierMovie:String, identifierUser:String, title:String, valorationMovie:Double,time:String) extends Serializable{

  var idMovie = identifierMovie
  var idUser = identifierUser
  var valoration = valorationMovie
  var timeValoration = time
  var titleMovie = title

}
