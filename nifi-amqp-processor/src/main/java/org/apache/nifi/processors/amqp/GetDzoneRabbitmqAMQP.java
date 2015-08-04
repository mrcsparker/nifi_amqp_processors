/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.amqp;

import static org.apache.nifi.processors.amqp.util.AmqpProperties.ACKNOWLEDGEMENT_MODE;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.AMQP_PROPS_TO_ATTRIBUTES;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.BATCH_SIZE;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.CLIENT_ID_PREFIX;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.DESTINATION_NAME;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.DESTINATION_TYPE;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.EXCHANGE_NAME;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.DESTINATION_VHOST;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.MESSAGE_SELECTOR;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.ROUTING_KEY;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.SERVICE_PROVIDER;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.SSL_CONTEXT_SVC;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.TIMEOUT;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.URL;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.VIRTUAL_HOST;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

import javax.jms.JMSException;
import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.Session;

import org.apache.commons.lang.SerializationUtils;
import org.apache.nifi.processors.amqp.util.JmsProcessingSummary;
import org.apache.nifi.processors.amqp.util.QpidAmqpFactory;
import org.apache.nifi.processors.amqp.util.WrappedMessageConsumer;
import org.apache.nifi.processors.amqp.util.KeyStoreToJKS;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.util.IntegerHolder;
import org.apache.qpid.jms.Message;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.AMQP.BasicProperties;



@EventDriven
@SideEffectFree
@Tags({ "amqp", "listen", "consume", "ssl", "queue", "topic" })
@CapabilityDescription("Pulls messages from an AMQP Queue, creating a FlowFile for each AMQP Message or bundle of messages")
public class GetDzoneRabbitmqAMQP extends AbstractProcessor implements Consumer {

	private final Queue<WrappedMessageConsumer> consumerQueue = new LinkedBlockingQueue<>();
	private volatile String keystore;
	private volatile String keystorePasswd;
	private volatile String truststore;
	private volatile String truststorePasswd;
	private volatile ProcessorLog logger;
	private volatile ProcessSession session;
	private volatile ProcessContext context;
	
	
	protected Channel channel;
	protected Connection connection;
	protected String endPointName;

    public static final String MAP_MESSAGE_PREFIX = "jms.mapmessage.";

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are routed to success")
            .build();

    private final Set<Relationship> relationships;
    private final List<PropertyDescriptor> propertyDescriptors;

    public GetDzoneRabbitmqAMQP() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(rels);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SERVICE_PROVIDER);
        descriptors.add(URL);
        descriptors.add(VIRTUAL_HOST);
        descriptors.add(EXCHANGE_NAME);
        descriptors.add(DESTINATION_NAME);
        descriptors.add(DESTINATION_TYPE);
        descriptors.add(ROUTING_KEY);
        descriptors.add(TIMEOUT);
        descriptors.add(BATCH_SIZE);
      //  descriptors.add(USERNAME);
      //  descriptors.add(PASSWORD);
        descriptors.add(ACKNOWLEDGEMENT_MODE);
        descriptors.add(MESSAGE_SELECTOR);
        descriptors.add(AMQP_PROPS_TO_ATTRIBUTES);
        descriptors.add(CLIENT_ID_PREFIX);
        this.propertyDescriptors = Collections.unmodifiableList(descriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }
	

	@OnScheduled
	public void inintializeSSSL(ProcessContext context)
			throws GeneralSecurityException, IOException {
		SSLContextService sslCntxtSvc = context.getProperty(SSL_CONTEXT_SVC)
				.asControllerService(SSLContextService.class);
		if (sslCntxtSvc != null) {
			if (!sslCntxtSvc.isTrustStoreConfigured()) {
				throw new IllegalStateException(
						"Chosen SSL Context Service does not have a TrustStore configured");
			}
			final String keystoreType = sslCntxtSvc.getKeyStoreType();
			keystore = sslCntxtSvc.getKeyStoreFile();
			keystorePasswd = sslCntxtSvc.getKeyStorePassword();
			if (sslCntxtSvc.isKeyStoreConfigured()) {
				keystore = sslCntxtSvc.getKeyStoreFile();
				keystorePasswd = sslCntxtSvc.getKeyStorePassword();
				if (!keystoreType.equals("JKS")) {
					final Path dir = Paths.get("conf/amqp");
					if (!Files.exists(dir)) {
						Files.createDirectory(dir);
					}
					keystore = KeyStoreToJKS.convertToJKS(keystore,
							keystorePasswd, dir, getIdentifier());
				}
			}
			final String truststoreType = sslCntxtSvc.getTrustStoreType();
			truststore = sslCntxtSvc.getTrustStoreFile();
			truststorePasswd = sslCntxtSvc.getTrustStorePassword();
			if (!truststoreType.equals("JKS")) {
				final Path dir = Paths.get("conf/amqp");
				if (!Files.exists(dir)) {
					Files.createDirectory(dir);
				}
				truststore = KeyStoreToJKS.convertToJKS(truststore,
						truststorePasswd, dir, getIdentifier());
			}
		} else {
			keystore = null;
			keystorePasswd = null;
			truststore = null;
			truststorePasswd = null;
		}
	}

	@OnStopped
	public void cleanupResources() {
		WrappedMessageConsumer wrappedConsumer = consumerQueue.poll();
		while (wrappedConsumer != null) {
			wrappedConsumer.close(getLogger());
			wrappedConsumer = consumerQueue.poll();
		}
	}

	@Override
	public void onTrigger(final ProcessContext context,
			final ProcessSession session) throws ProcessException {
		final ProcessorLog logger = getLogger();
		
		this.logger = logger;
		this.session = session;
		this.context = context;

		final String destinationName = context.getProperty(DESTINATION_NAME)
				.getValue();
		final String exchangeName = context.getProperty(EXCHANGE_NAME)
				.getValue();
		//final String virtualHost = context.getProperty(DESTINATION_VHOST)
			//	.getValue();
		final String routingKey = context.getProperty(ROUTING_KEY).getValue();
		final String uri = context.getProperty(URL).getValue();

		RabbitConsume("dzone-queue", context, session, logger);

		

	}

	
	public void RabbitConsume(String endPointName, final ProcessContext context, final ProcessSession session, ProcessorLog logger) {
		try{
			this.logger = logger;
			this.session = session;
			this.context = context;
			
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
		String message = (String) map.get("message number");
		logger.info("Message number " + map.get("message number") + " received");
		
		doWork(session, logger, body, message);
		
		channel.basicAck(env.getDeliveryTag(), false);
	}
	
	
	public void handleCancel(String consumerTag){}
	public void handleCancelOk(String consumerTag){}
	public void handleRecoverOk(String consumerTag){}
	public void handleShutdownSignal(String consumerTag, ShutdownSignalException arg1){}
	
	private void doWork(ProcessSession session, ProcessorLog logger,
			final byte[] byteMessage, String message) {
		// TODO Auto-generated method stub

		// String messageType = props.getType();
		try {
			logger.info(" [x] Received '" + message + "'");
			// + " of Message Type " + messageType);

			final IntegerHolder msgsThisFlowFile = new IntegerHolder(1);

			FlowFile flowFile = session.create();
			try {
				// MapMessage is exception, add only name-value pairs to
				// FlowFile attributes
				// all other message types, write Message body to FlowFile
				// content
				flowFile = session.write(flowFile, new OutputStreamCallback() {
					@Override
					public void process(final OutputStream rawOut)
							throws IOException {
						try (final OutputStream out = new BufferedOutputStream(
								rawOut, 65536)) {
							// final byte[] messageBody =
							// QpidAmqpFactory.createByteArray(message);

							// final byte[] messageBody = delivery.getBody();
							out.write(byteMessage);
						}
					}
				});

				// if (addAttributes) {
				// flowFile = session.putAllAttributes(flowFile,
				// QpidAmqpFactory.createAttributeMap(message));
				// }

				// session.getProvenanceReporter().receive(flowFile,
				// context.getProperty(URL).getValue());
				session.transfer(flowFile, REL_SUCCESS);
				logger.info(
						"Created {} from {} messages received from JMS Server and transferred to 'success'",
						new Object[] { flowFile, msgsThisFlowFile.get() });

				// return new JmsProcessingSummary(flowFile.getSize(),
				// message, flowFile);
			} catch (Exception e) {
				session.remove(flowFile);
				throw e;
			}
		} 
		finally {
		}
		
		
	}
}
