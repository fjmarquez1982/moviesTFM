package es.simulator.movie

import es.common.sender.kafka.KafkaSender

import scala.io.Source

class SendMovies {


  def sendMovies(): Unit = {

    val fileName = "/home/netkako/Documentos/ProyectoTFM/dataset/movies.csv"
    val buffer = Source.fromFile(fileName)
    val kafkaSender = new KafkaSender("movies-topic","movies-client")
    for (line <- buffer.getLines().drop(1)){
      kafkaSender.sendToKafka(line)
      Thread.sleep(1000)
    }
    buffer.close()
  }
}
