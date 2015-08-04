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
import static org.apache.nifi.processors.amqp.util.AmqpProperties.ACK_MODE_CLIENT;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.BATCH_SIZE;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.CLIENT_ID_PREFIX;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.DESTINATION_NAME;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.AMQP_PROPS_TO_ATTRIBUTES;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.DESTINATION_TYPE;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.EXCHANGE_NAME;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.ROUTING_KEY;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.SERVICE_PROVIDER;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.MESSAGE_SELECTOR;
//import static org.apache.nifi.processors.amqp.util.AmqpProperties.PASSWORD;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.TIMEOUT;
import static org.apache.nifi.processors.amqp.util.AmqpProperties.URL;
//import static org.apache.nifi.processors.amqp.util.AmqpProperties.USERNAME;

import static org.apache.nifi.processors.amqp.util.AmqpProperties.VIRTUAL_HOST;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;

import org.apache.commons.lang.SerializationUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processors.amqp.util.QpidAmqpFactory;
import org.apache.nifi.processors.amqp.util.JmsProcessingSummary;
import org.apache.nifi.processors.amqp.util.WrappedMessageConsumer;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.util.IntegerHolder;
import org.apache.nifi.util.StopWatch;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.AMQP.BasicProperties;

public abstract class RabbitmqConsumer extends AbstractProcessor implements Consumer {
	
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

    public RabbitmqConsumer() {
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

    public void consume(final ProcessContext context, final ProcessSession session, final WrappedMessageConsumer wrappedConsumer) throws ProcessException {
        final ProcessorLog logger = getLogger();

        final MessageConsumer consumer = wrappedConsumer.getConsumer();
        final boolean clientAcknowledge = context.getProperty(ACKNOWLEDGEMENT_MODE).getValue().equalsIgnoreCase(ACK_MODE_CLIENT);
        final long timeout = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
        final boolean addAttributes = context.getProperty(AMQP_PROPS_TO_ATTRIBUTES).asBoolean();
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();

        final JmsProcessingSummary processingSummary = new JmsProcessingSummary();

        final StopWatch stopWatch = new StopWatch(true);
        for (int i = 0; i < batchSize; i++) {

            final Message message;
            try {
                // If we haven't received a message, wait until one is available. If we have already received at least one
                // message, then we are not willing to wait for more to become available, but we are willing to keep receiving
                // all messages that are immediately available.
                if (processingSummary.getMessagesReceived() == 0) {
                    message = consumer.receive(timeout);
                } else {
                    message = consumer.receiveNoWait();
                }
            } catch (final JMSException e) {
                logger.error("Failed to receive JMS Message due to {}", e);
                wrappedConsumer.close(logger);
                break;
            }

            if (message == null) { // if no messages, we're done
                break;
            }

            try {
                processingSummary.add(map2FlowFile(context, session, message, addAttributes, logger));
            } catch (Exception e) {
                logger.error("Failed to receive JMS Message due to {}", e);
                wrappedConsumer.close(logger);
                break;
            }
        }

        if (processingSummary.getFlowFilesCreated() == 0) {
            context.yield();
            return;
        }

        session.commit();

        stopWatch.stop();
        if (processingSummary.getFlowFilesCreated() > 0) {
            final float secs = ((float) stopWatch.getDuration(TimeUnit.MILLISECONDS) / 1000F);
            float messagesPerSec = ((float) processingSummary.getMessagesReceived()) / secs;
            final String dataRate = stopWatch.calculateDataRate(processingSummary.getBytesReceived());
            logger.info("Received {} messages in {} milliseconds, at a rate of {} messages/sec or {}",
                    new Object[]{processingSummary.getMessagesReceived(), stopWatch.getDuration(TimeUnit.MILLISECONDS), messagesPerSec, dataRate});
        }

        // if we need to acknowledge the messages, do so now.
        final Message lastMessage = processingSummary.getLastMessageReceived();
        if (clientAcknowledge && lastMessage != null) {
            try {
                lastMessage.acknowledge();  // acknowledge all received messages by acknowledging only the last.
            } catch (final JMSException e) {
                logger.error("Failed to acknowledge {} JMS Message(s). This may result in duplicate messages. Reason for failure: {}",
                        new Object[]{processingSummary.getMessagesReceived(), e});
            }
        }
    }

    public static JmsProcessingSummary map2FlowFile(final ProcessContext context, final ProcessSession session, final Message message, final boolean addAttributes, ProcessorLog logger)
            throws Exception {

        // Currently not very useful, because always one Message == one FlowFile
        final IntegerHolder msgsThisFlowFile = new IntegerHolder(1);

        FlowFile flowFile = session.create();
        try {
            // MapMessage is exception, add only name-value pairs to FlowFile attributes
            if (message instanceof MapMessage) {
                MapMessage mapMessage = (MapMessage) message;
                flowFile = session.putAllAttributes(flowFile, createMapMessageValues(mapMessage));
            } else { // all other message types, write Message body to FlowFile content
                flowFile = session.write(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream rawOut) throws IOException {
                        try (final OutputStream out = new BufferedOutputStream(rawOut, 65536)) {
                            final byte[] messageBody = QpidAmqpFactory.createByteArray(message);
                            out.write(messageBody);
                        } catch (final JMSException e) {
                            throw new ProcessException("Failed to receive JMS Message due to {}", e);
                        }
                    }
                });
            }

            if (addAttributes) {
                flowFile = session.putAllAttributes(flowFile, QpidAmqpFactory.createAttributeMap(message));
            }

            session.getProvenanceReporter().receive(flowFile, context.getProperty(URL).getValue());
            session.transfer(flowFile, REL_SUCCESS);
            logger.info("Created {} from {} messages received from JMS Server and transferred to 'success'",
                    new Object[]{flowFile, msgsThisFlowFile.get()});

            return new JmsProcessingSummary(flowFile.getSize(), message, flowFile);

        } catch (Exception e) {
            session.remove(flowFile);
            throw e;
        }
    }

    public static Map<String, String> createMapMessageValues(final MapMessage mapMessage) throws JMSException {
        final Map<String, String> valueMap = new HashMap<>();

        final Enumeration<?> enumeration = mapMessage.getMapNames();
        while (enumeration.hasMoreElements()) {
            final String name = (String) enumeration.nextElement();

            final Object value = mapMessage.getObject(name);
            if (value == null) {
                valueMap.put(MAP_MESSAGE_PREFIX + name, "");
            } else {
                valueMap.put(MAP_MESSAGE_PREFIX + name, value.toString());
            }
        }

        return valueMap;
    }
    
    ProcessorLog logger;
/*
	/public QueueConsumer(String endPointName, ProcessorLog logger) throws IOException,TimeoutException {
		super(endPointName);
		this.logger = logger;
	}
*/
	public void RabbitConsume(String endPointName, final ProcessContext context, final ProcessSession session, ProcessorLog logger) {
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
