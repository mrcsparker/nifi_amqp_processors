package org.apache.nifi.processors.amqp.util;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public abstract class EndPoint {
	
	protected Channel channel;
	protected Connection connection;
	protected String endPointName;
	
	public EndPoint(String endPointName)throws IOException, TimeoutException{
		
		ConnectionFactory factory = new ConnectionFactory();
		
		factory.setHost("192.168.0.28");
		
		connection = factory.newConnection();
		
		channel = connection.createChannel();
		
		channel.queueDeclare(endPointName, true, false, false, null);
	}
	
	public void close() throws IOException, TimeoutException{
		this.channel.close();
		this.connection.close();
	}

}

