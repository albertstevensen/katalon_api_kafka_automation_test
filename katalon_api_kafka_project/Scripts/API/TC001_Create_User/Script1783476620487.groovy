import com.kms.katalon.core.testobject.RequestObject
import com.kms.katalon.core.testobject.ResponseObject
import com.kms.katalon.core.testobject.TestObjectProperty
import com.kms.katalon.core.testobject.ConditionType
import com.kms.katalon.core.testobject.impl.HttpTextBodyContent
import com.kms.katalon.core.webservice.keyword.WSBuiltInKeywords as WS
import internal.GlobalVariable
import groovy.json.JsonSlurper

String name = 'Albert Stevensen'
String email = 'albert.test@example.com'
String role = 'automation tester'

String requestBody = """
{
  "name": "${name}",
  "email": "${email}",
  "role": "${role}"
}
"""

RequestObject request = new RequestObject('Create User')
request.setRestUrl(GlobalVariable.baseUrl + '/api/users')
request.setRestRequestMethod('POST')
request.setHttpHeaderProperties([
	new TestObjectProperty('Content-Type', ConditionType.EQUALS, 'application/json')
])
request.setBodyContent(new HttpTextBodyContent(requestBody, 'UTF-8', 'application/json'))

ResponseObject response = WS.sendRequest(request)

WS.verifyResponseStatusCode(response, 201)

def jsonResponse = new JsonSlurper().parseText(response.getResponseBodyContent())

assert jsonResponse.message == 'User created successfully'
assert jsonResponse.data.id != null
assert jsonResponse.data.name == name
assert jsonResponse.data.email == email
assert jsonResponse.data.role == role

GlobalVariable.createdUserId = jsonResponse.data.id
GlobalVariable.createdUserEmail = jsonResponse.data.email

println('Created User ID: ' + GlobalVariable.createdUserId)
println('Created User Email: ' + GlobalVariable.createdUserEmail)
println('Response Body: ' + response.getResponseBodyContent())