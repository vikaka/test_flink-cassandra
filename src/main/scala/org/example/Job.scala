package org.example

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import java.util.Properties

import com.datastax.driver.core.Cluster

import collection.JavaConverters._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.cassandra.{CassandraPojoSink, CassandraSink, CassandraTupleSink, ClusterBuilder}


object Job {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val serdeSchema = new SimpleStringSchema()
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "ec2-34-203-160-240.compute-1.amazonaws.com:9092")
    properties.setProperty("zookeeper.connect", "ec2-34-203-160-240.compute-1.amazonaws.com:2181")
    properties.setProperty("group.id", "test")
    val stream = env.addSource(new FlinkKafkaConsumer010[String]("test", serdeSchema, properties)).print()

    val INSERT = "INSERT INTO dev.emp (test_text) VALUES (?)"

    val list = List(new Tuple1("from flink with love"), new Tuple1("testing from flink"))
    val source = env.fromCollection(list.asJava)

      CassandraSink.addSink(source)
      .setQuery(INSERT)
      .setClusterBuilder(new ClusterBuilder() {
        override def buildCluster(builder: Cluster.Builder): Cluster = {
          builder.addContactPoint("34.202.147.37").build()
        }
      })
      .build()
          env.execute("Flink Kafka Example")


  }

}