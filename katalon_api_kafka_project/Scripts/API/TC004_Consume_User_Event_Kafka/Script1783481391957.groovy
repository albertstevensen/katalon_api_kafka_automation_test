import internal.GlobalVariable
import groovy.json.JsonSlurper

assert GlobalVariable.createdUserId != null
assert GlobalVariable.createdUserId != ''
assert GlobalVariable.createdUserEmail != null
assert GlobalVariable.createdUserEmail != ''

String kafkaMessage = CustomKeywords.'kafka.KafkaConsumerKeyword.consumeUserCreatedEvent'(
	GlobalVariable.kafkaBootstrapServer,
	GlobalVariable.kafkaTopic,
	GlobalVariable.kafkaGroupId,
	GlobalVariable.createdUserId,
	15
)

def jsonMessage = new JsonSlurper().parseText(kafkaMessage)

assert jsonMessage.eventType == 'USER_CREATED'
assert jsonMessage.userId == GlobalVariable.createdUserId
assert jsonMessage.email == GlobalVariable.createdUserEmail
assert jsonMessage.source == 'api-service'

println('Kafka validation success')
println('Kafka Event Type: ' + jsonMessage.eventType)
println('Kafka User ID: ' + jsonMessage.userId)
println('Kafka Email: ' + jsonMessage.email)