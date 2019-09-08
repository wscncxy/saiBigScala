package scala

import java.time.Duration
import java.util.Properties
import java.util.Arrays

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}

object Hello extends App {
  val groupid = "oyp_test24" //kafka有一个注意点，就是同一个组的消费者不会再取topic中已经消费过的数据
  val consumerid = "oyp_consumer"
  val topic = "ase-info" //取数据源的topic
  val props: Properties = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "47.52.27.206:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "zhouxiang")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
  props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10")
  val consumer = new KafkaConsumer[String, String](props) //创建一个消费者
  consumer.subscribe(Arrays.asList("sai-big-data-topic"))
  while (true) {
    val consumerRecords = consumer.poll(Duration.ofMillis(10))
    val iterator = consumerRecords.iterator();
    while (iterator.hasNext()) {
      val record = iterator.next();
      println(record.value());
    }
  }
  println("结束")
}