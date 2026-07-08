package kafka

import com.kms.katalon.core.annotation.Keyword
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util.Collections
import java.util.Properties

class KafkaConsumerKeyword {

	@Keyword
	String consumeUserCreatedEvent(String bootstrapServer, String topic, String groupId, String expectedUserId, int timeoutSeconds) {
		Properties props = new Properties()

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId + '-' + System.currentTimeMillis())
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 'earliest')
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, 'true')

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)
		consumer.subscribe(Collections.singletonList(topic))

		long endTime = System.currentTimeMillis() + (timeoutSeconds * 1000)

		try {
			while (System.currentTimeMillis() < endTime) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000))

				for (ConsumerRecord<String, String> record : records) {
					String messageValue = record.value()

					println('Kafka message received: ' + messageValue)

					if (messageValue != null && messageValue.contains(expectedUserId)) {
						return messageValue
					}
				}
			}

			throw new AssertionError('Kafka message not found for userId: ' + expectedUserId)
		} finally {
			consumer.close()
		}
	}
}