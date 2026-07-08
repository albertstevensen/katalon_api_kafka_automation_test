import com.kms.katalon.core.testobject.RequestObject
import com.kms.katalon.core.testobject.ResponseObject
import com.kms.katalon.core.webservice.keyword.WSBuiltInKeywords as WS
import internal.GlobalVariable
import groovy.json.JsonSlurper

RequestObject request = new RequestObject('Get All Users')
request.setRestUrl(GlobalVariable.baseUrl + '/api/users')
request.setRestRequestMethod('GET')

ResponseObject response = WS.sendRequest(request)

WS.verifyResponseStatusCode(response, 200)

def jsonResponse = new JsonSlurper().parseText(response.getResponseBodyContent())

assert jsonResponse.message == 'Users retrieved successfully'
assert jsonResponse.data instanceof List
assert jsonResponse.data.size() > 0

println('Total users: ' + jsonResponse.data.size())
println('Response Body: ' + response.getResponseBodyContent())