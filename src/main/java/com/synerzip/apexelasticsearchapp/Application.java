/**
 * Put your copyright and license info here.
 */
package com.synerzip.apexelasticsearchapp;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.elasticsearch.ElasticSearchConnectable;
import com.datatorrent.contrib.elasticsearch.ElasticSearchMapOutputOperator;
import com.typesafe.config.ConfigFactory;

import org.apache.apex.malhar.kafka.AbstractKafkaInputOperator;
import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Map;

@ApplicationAnnotation(name="ApexElasticSearchApplication")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaSinglePortInputOperator kafkaIn = dag.addOperator("kafkaIn", new KafkaSinglePortInputOperator());
    kafkaIn.setInitialOffset(AbstractKafkaInputOperator.InitialOffset.EARLIEST.name());
    RequestFilterAggregator requestFilterAggregator = dag.addOperator("requestFilterAggregator", new RequestFilterAggregator());
    ElasticSearchMapOutputOperator<Map<String, Object>> elasticOut = dag.addOperator("elasticOut", new ElasticSearchMapOutputOperator<Map<String, Object>>());
    configureElasticSearch(elasticOut);

    dag.addStream("KafkaToProcessTuple", kafkaIn.outputPort, requestFilterAggregator.inputPort);
    dag.addStream("RequestFilterAggregatorToElastic", requestFilterAggregator.outputPortElastic, elasticOut.input);

    dag.setAttribute(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS, 5000);
    dag.setAttribute(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 12);
  }

  ElasticSearchConnectable createStore() throws IOException {
    String host = ConfigFactory.load().getString("elastic-search.hostname");
    int port = ConfigFactory.load().getInt("elastic-search.port");
    String clusterName = ConfigFactory.load().getString("elastic-search.cluster-name");

    ElasticSearchConnectable store = new ElasticSearchConnectable();
    store.setHostName(host);
    store.setPort(port);
    store.setClusterName(clusterName);
    store.connect();
    return store;
  }

  void configureElasticSearch(ElasticSearchMapOutputOperator<Map<String, Object>> elasticOut) {
    String idField = ConfigFactory.load().getString("elastic-search.id-field");
    String indexName = ConfigFactory.load().getString("elastic-search.index-name");
    String typeName = ConfigFactory.load().getString("elastic-search.type");

    elasticOut.setIndexName(indexName);
    elasticOut.setType(typeName);
    elasticOut.setIdField(idField);
    try {
      elasticOut.setStore(createStore());
    } catch (IOException e) {
      e.printStackTrace();
    }
    try {
      elasticOut.setStore(createStore());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}