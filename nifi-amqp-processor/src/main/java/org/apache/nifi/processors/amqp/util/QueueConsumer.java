package org.apache.nifi.processors.amqp.util;

import static org.apache.nifi.processors.amqp.util.AmqpProperties.URL;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;

import org.apache.commons.lang.SerializationUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.util.IntegerHolder;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

public class QueueConsumer extends EndPoint implements Runnable, Consumer {
	
	ProcessorLog logger;

	public QueueConsumer(String endPointName, ProcessorLog logger) throws IOException,TimeoutException {
		super(endPointName);
		this.logger = logger;
	}

	public void run() {
		try{
			
			channel.basicQos(1);
			channel.basicConsume("dzone-queue", true, this);
			channel.close();
			connection.close();
			
		}catch (IOException | TimeoutException e){
			logger.info("Caught IO Exception " + e);
		}
	}
	
	public void handleConsumeOk(String consumerTag){
		//logger.info("consumer " +consumerTag + " registered");
	}
	
	public void handleDelivery(String consumerTag, Envelope env, BasicProperties props, byte[] body) throws IOException{
		Map map = (HashMap)SerializationUtils.deserialize(body);
		logger.info("Message number " + map.get("message number") + " received");
		
		
		
		channel.basicAck(env.getDeliveryTag(), false);
	}
	
	
	public void handleCancel(String consumerTag){}
	public void handleCancelOk(String consumerTag){}
	public void handleRecoverOk(String consumerTag){}
	public void handleShutdownSignal(String consumerTag, ShutdownSignalException arg1){}
	
	
	 
	
}

