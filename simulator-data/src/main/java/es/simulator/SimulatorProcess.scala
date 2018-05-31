package es.simulator

import es.simulator.movie.SendMovies
import es.simulator.valoration.SendValorations

/**
  *
  */
object SimulatorProcess{


  def main(args: Array[String]): Unit = {


    var typeSimulator = args(0)
    if(typeSimulator == "MOVIE"){
      val simulatorMovie = new SendMovies
      simulatorMovie.sendMovies()
    }
    else if(typeSimulator == "VALORATION"){
      val simulatorValoration = new SendValorations
      simulatorValoration.sendValorations()
    }
  }

}
