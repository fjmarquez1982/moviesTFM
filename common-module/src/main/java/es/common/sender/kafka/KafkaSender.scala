package es.common.sender.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class KafkaSender (topicToSend:String, clientId:String){

  val props = new Properties()
  props.put("bootstrap.servers","localhost:9092")
  props.put("client.id",clientId)
  props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
  val topic = topicToSend
  val producer = new KafkaProducer[String, String](props)

  def sendToKafka(line: String): Unit ={
    println("Enviando "+line)
    producer.send(new ProducerRecord[String, String](topic, line))
    println("Enviado!!!!!")
  }
}
