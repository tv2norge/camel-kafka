/**
 *
 */
package org.giwi.camel.kafka.component;

import kafka.consumer.KafkaStream;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import org.apache.camel.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Giwi Softwares
 *
 */
public class Klistener implements Runnable {

	private KafkaStream<Message> stream;
	private KafkaEndpoint endpoint;
	private KafkaConsumer consumer;
	private static final Logger LOG = LoggerFactory.getLogger(Klistener.class);

	/*
	 * (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		for (final MessageAndMetadata<Message> message : stream) {
			final Exchange exchange = endpoint.createExchange();
			final byte[] bytes = new byte[message.message().payload().remaining()];
			message.message().payload().get(bytes);
			exchange.getIn().setBody(bytes);
			try {
				consumer.getProcessor().process(exchange);
			} catch (Exception e) {
				exchange.setException(e);
				LOG.error(e.getMessage(), e);
			} finally {
				if (exchange.getException() != null) {
					consumer.getExceptionHandler().handleException("Error processing exchange", exchange, exchange.getException());
				}
			}
		}

	}

	/**
	 * @return the stream
	 */
	public final KafkaStream<Message> getStream() {
		return stream;
	}

	/**
	 * @param stream
	 *    the stream to set
	 */
	public final void setStream(final KafkaStream<Message> stream) {
		this.stream = stream;
	}

	/**
	 * @return the consumer
	 */
	public final KafkaConsumer getConsumer() {
		return consumer;
	}

	/**
	 * @param consumer
	 *     the consumer to set
	 */
	public final void setConsumer(final KafkaConsumer consumer) {
		this.consumer = consumer;
	}

	/**
	 * @return the endpoint
	 */
	public final KafkaEndpoint getEndpoint() {
		return endpoint;
	}

	/**
	 * @param endpoint
	 *    the endpoint to set
	 */
	public final void setEndpoint(final KafkaEndpoint endpoint) {
		this.endpoint = endpoint;
	}

}
