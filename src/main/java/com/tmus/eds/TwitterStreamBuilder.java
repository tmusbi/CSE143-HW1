/**********************************************************************
 * Copyright (c) 2016 T-MOBILE USA, INC. All rights reserved.
 * T-MOBILE USA PROPRIETARY/CONFIDENTIAL.
 **********************************************************************/
package com.tmus.eds;

import java.io.IOException;
import java.util.Properties;

import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * @author rwu, @date Sep 30, 2016 3:56:16 PM
 *
 */
public class TwitterStreamBuilder {

	public static TwitterStream getStream() throws IOException {
		
		Properties configs = new Properties();
		configs.load(TwitterStreamBuilder.class.getResourceAsStream("/application.properties"));
		ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setJSONStoreEnabled(true);
        cb.setOAuthConsumerKey(configs.getProperty(Constants.TWITTER_CONSUMER_KEY));
        cb.setOAuthConsumerSecret(configs.getProperty(Constants.TWITTER_CONSUMER_SECRECT));
        cb.setOAuthAccessToken(configs.getProperty(Constants.TWITTER_ACCESS_TOKEN));
        cb.setOAuthAccessTokenSecret(configs.getProperty(Constants.TWITTER_ACCESS_TOKEN_SECRET));
       
        return new TwitterStreamFactory(cb.build()).getInstance();
	}
}
