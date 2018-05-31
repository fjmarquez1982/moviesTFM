package es.simulator.valoration

import es.common.sender.kafka.KafkaSender

import scala.io.Source

class SendValorations {

  def sendValorations(): Unit = {

    val fileName = "/home/netkako/Documentos/ProyectoTFM/dataset/ratings_small.csv"
    val buffer = Source.fromFile(fileName)
    val kafkaSender = new KafkaSender("valorations-topic", "valorations-client")
    for (line <- buffer.getLines().drop(1)){
      kafkaSender.sendToKafka(line)
      Thread.sleep(1000)
    }
    buffer.close()
  }
}
