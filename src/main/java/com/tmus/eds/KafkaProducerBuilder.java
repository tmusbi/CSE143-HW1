/**********************************************************************
 * Copyright (c) 2016 T-MOBILE USA, INC. All rights reserved.
 * T-MOBILE USA PROPRIETARY/CONFIDENTIAL.
 **********************************************************************/
package com.tmus.eds;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

/**
 * @author rwu, @date Sep 30, 2016 4:28:27 PM
 *
 */
public class KafkaProducerBuilder {

	public static Producer<String, String> getProducer() throws IOException {
		Properties configs = new Properties();
		configs.load(KafkaProducerBuilder.class.getResourceAsStream("/application.properties"));
		Properties props = new Properties();
		props.put("bootstrap.servers", configs.getProperty(Constants.KAFKA_BROKER_LIST));
		props.put("ack", configs.getProperty(Constants.KAFKA_REQUEST_REQUIRED_ACKS));
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		return producer;
	}
}
