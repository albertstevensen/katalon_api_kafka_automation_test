import com.kms.katalon.core.testobject.RequestObject
import com.kms.katalon.core.testobject.ResponseObject
import com.kms.katalon.core.webservice.keyword.WSBuiltInKeywords as WS
import internal.GlobalVariable
import groovy.json.JsonSlurper

assert GlobalVariable.createdUserId != null
assert GlobalVariable.createdUserId != ''

RequestObject request = new RequestObject('Get User By ID')
request.setRestUrl(GlobalVariable.baseUrl + '/api/users/' + GlobalVariable.createdUserId)
request.setRestRequestMethod('GET')

ResponseObject response = WS.sendRequest(request)

WS.verifyResponseStatusCode(response, 200)

def jsonResponse = new JsonSlurper().parseText(response.getResponseBodyContent())

assert jsonResponse.message == 'User retrieved successfully'
assert jsonResponse.data.id == GlobalVariable.createdUserId
assert jsonResponse.data.email == GlobalVariable.createdUserEmail

println('Retrieved User ID: ' + jsonResponse.data.id)
println('Retrieved User Email: ' + jsonResponse.data.email)
println('Response Body: ' + response.getResponseBodyContent())