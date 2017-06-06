package org.example

import com.datastax.driver.core.Cluster
import org.apache.flink.api.java.tuple.Tuple1
import org.apache.flink.streaming.api.scala._
import collection.JavaConverters._
import com.datastax.driver.core.Cluster.Builder
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.cassandra.{ClusterBuilder, CassandraSink}

object Job {
  def main(args: Array[String]) {

    val INSERT = "INSERT INTO dev.emp (text_test) VALUES (?)"

    val list = List(new Tuple1("From Flink with Love"))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.fromCollection(list.asJava)

    CassandraSink.addSink(source)
      .setQuery(INSERT)
      .setClusterBuilder(new ClusterBuilder() {
        override def buildCluster(builder: Builder): Cluster = {

          builder.addContactPoint("34.202.147.37").build()
        }
      })
      .build()

    env.execute("WriteTupleIntoCassandra")
  }
}