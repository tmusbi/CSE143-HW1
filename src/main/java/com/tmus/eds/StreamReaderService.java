/**********************************************************************
 * Copyright (c) 2016 T-MOBILE USA, INC. All rights reserved.
 * T-MOBILE USA PROPRIETARY/CONFIDENTIAL.
 **********************************************************************/
package com.tmus.eds;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * @author rwu, @date Sep 30, 2016 3:27:48 PM
 *
 */
public class StreamReaderService {
	
	public void readTwitterFeed() throws IOException {
		Properties configs = new Properties();
		configs.load(StreamReaderService.class.getResourceAsStream("/application.properties"));
		System.out.println("kafka topic: " + configs.getProperty(Constants.KAFKA_TOPIC));
		TwitterStream stream = TwitterStreamBuilder.getStream();
		Producer<String, String> producer = KafkaProducerBuilder.getProducer();
		
		StatusListener listener = new StatusListener() {

			@Override
			public void onException(Exception ex) {
				System.out.println("Exception occured:" + ex.getMessage());
				ex.printStackTrace();
			}

			@Override
			public void onStatus(Status status) {
				//System.out.println("Got twit:" + status.getText());
				//TwitterStreamBean bean = new TwitterStreamBean();
				System.out.println("kafka topic: " + configs.getProperty(Constants.KAFKA_TOPIC));
				ProducerRecord<String, String> record = new ProducerRecord<String, String>(configs.getProperty(Constants.KAFKA_TOPIC), String.valueOf(status.getId()), TwitterObjectFactory.getRawJSON(status));
				producer.send(record);
				
				System.out.println("@" + status.getUser().getScreenName() 
						//+ " & " + status.getUser()
						+ " - " + status.getText() 
						+ " => " + status.getId());
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				// TODO Auto-generated method stub
				System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				// TODO Auto-generated method stub
				System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				// TODO Auto-generated method stub
				System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
			}

			@Override
			public void onStallWarning(StallWarning warning) {
				// TODO Auto-generated method stub
				System.out.println("Got stall warning:" + warning);
			}
		};
		
		stream.addListener(listener);
        stream.sample();
	}

	public static void main(String[] args) throws TwitterException, IOException {
		StreamReaderService service = new StreamReaderService();
		service.readTwitterFeed();
	}
}
