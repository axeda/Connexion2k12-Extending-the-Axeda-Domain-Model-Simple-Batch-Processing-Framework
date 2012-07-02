import com.axeda.platform.sdk.v1.services.extobject.ExtendedObject
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObjectService
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObjectSearchCriteria
import groovy.json.JsonOutput
import com.axeda.drm.sdk.scripto.Request
import groovy.json.JsonSlurper
import com.axeda.platform.sdk.v1.services.ServiceFactory
import net.sf.json.JSONObject
import java.beans.PropertyChangeListener
import org.apache.commons.lang.exception.ExceptionUtils

/**
 * GetBatch
 *
 * GetBatch returns the state of a single batch.
 *
 * Request Contract:
 *
 * {
 *     "criteria": { "id": 1337 }
 * }
 *
 * Success Response Contract:
 *
 * {
 *     "resultType": "success",
 *     "result": {
 *          "id": 1337,
 *          "name": "Example Batch",
 *          "inceptionTime": 0,
 *          "state": "NOT_STARTED",
 *          "completedCount": 0,
 *          "successCount": 0,
 *          "errorCount": 0,
 *     }
 * }
 *
 *
 * Error Response Contract:
 *
 * {
 *      "resultType": "error",
 *      "result": [
 *          {
 *              "id": "1234",
 *              "time": 0,
 *              "message": "An example error."
 *          }
 *      ]
 * }
 *
 * @author philip lombardi <plombardi@axeda.com>
 */

String mock         = Request.parameters.mock
String mockError    = Request.parameters.mockError
String contentType  = "application/json"

//
// mock data
//
Map mockRequest = [
    criteria: [id: 102290]
]

Map mockResponse = [
    resultType: "success",
    result: [
        id: 1337,
        name: "Example Batch",
        inception: (new Date() - 1).time,
        state: "NOT_STARTED",
        completedCount: 0,
        successCount: 0,
        errorCount: 0
    ]
]

Map mockErrorResponse = [
    resultType: "error",
    result: [
        [
            name: "",
            id: 1234,
            time: new Date().time,
            reason: "An example error."
        ]
    ]
]

Map response = [:]
List errors = []

try {
    //
    // mock switches
    //
    if (mock)       { return createReturnMap(contentType, mockResponse) }
    if (mockError)  { return createReturnMap(contentType, mockErrorResponse) }

    //
    // service validations
    //
    def request = new JsonSlurper().parseText(Request.body) as Map
    if (!isValidRequest(request: request, errors: errors)) {
        return createReturnMap(contentType, createErrorResponse(response: response, errors: errors))
    }

    //
    // service implementation logic.
    //
    def eoSvc = new ServiceFactory().extendedObjectService
    def criteria = request.criteria

    def batchAnchor = findBatchAnchor(queryCriteria: criteria, errors: errors, eoSvc)
    def batchErrors = batchAnchor ? findBatchErrors(queryCriteria: criteria, eoSvc) : []
    def batchSuccesses = batchAnchor ? findBatchSuccesses(queryCriteria: criteria, eoSvc) : []

    response = createResponse(batch: createBatchMap(batchAnchor, batchSuccesses, batchErrors), errors: errors)
}
catch (any) {
    processFatalException(throwable: any, response: response)
}

return createReturnMap(contentType, response)

//
// Service Implementation Methods
//
private Boolean isValidRequest(args) {
    Boolean result = true

    def request = args.request as Map
    def errors = args.errors as ObservableList
    errors.addPropertyChangeListener( {
        if (it instanceof ObservableList.ElementAddedEvent) { result = false } } as PropertyChangeListener )

    request.criteria        ?: createError(errors, "1001", "Missing request object 'criteria'.")
    request.criteria?.id    ?: createError(errors, "1001", "Missing criteria field 'id'.")

    return result
}

private ExtendedObject findBatchAnchor(args, ExtendedObjectService eoSvc) {
    def criteria = new ExtendedObjectSearchCriteria()
    criteria.extendedObjectClassName = "connexion.batch.Anchor"
    criteria.id = args.queryCriteria.id as Long

    def queryResult = eoSvc.findExtendedObjects(criteria, -1, -1, null)[0]
    if (!queryResult) {
        createError(args.errors, "1000", "Unable to find batch with provided ID: '${args.queryCriteria.id}'.")
    }

    return queryResult
}

private List<ExtendedObject> findBatchErrors(args, ExtendedObjectService eoSvc){
    def criteria = new ExtendedObjectSearchCriteria()
    criteria.extendedObjectClassName = "connexion.batch.ErrorAnchor"
    criteria.internalObjectId = args.queryCriteria.id as Long
    return eoSvc.findExtendedObjects(criteria, -1, -1, null)
}

private List<ExtendedObject> findBatchSuccesses(args, ExtendedObjectService eoSvc) {
    def criteria = new ExtendedObjectSearchCriteria()
    criteria.extendedObjectClassName = "connexion.batch.SuccessAnchor"
    criteria.internalObjectId = args.queryCriteria.id as Long
    return eoSvc.findExtendedObjects(criteria, -1, -1, null)
}

private Map createBatchMap(
    ExtendedObject batchAnchor, List<ExtendedObject> batchSuccesses, List<ExtendedObject> batchErrors) {

    if (batchAnchor) {
        return [
            id: batchAnchor.id,
            name: batchAnchor.getPropertyByName("name").value,
            inceptionTime: batchAnchor.getPropertyByName("inceptionTime").value,
            completionTime: batchAnchor.getPropertyByName("completionTime").value,
            state: batchAnchor.getPropertyByName("state").value,
            size: batchAnchor.getPropertyByName("size").value as Integer,
            completedCount: (batchSuccesses + batchErrors).size(),
            successCount: batchSuccesses.size(),
            errorCount: batchErrors.size(),
            notifyEmail: batchAnchor.getPropertyByName("notifyEmail").value
        ]
    } else {
        return null
    }
}

//
// Service Utility Methods
//

private Map createReturnMap(String contentType, output) {
    return ['Content-Type': contentType, Content: JsonOutput.prettyPrint(JsonOutput.toJson(output))]
}

private Map processFatalException(args) {
    logError(message: ExceptionUtils.getFullStackTrace(args.throwable as Throwable))
    createError(args.errors, "0001", "Fatal execution exception. Check logs or contact your system administrator.")
}

private void createError(List errors, String errorID, String message) {
    errors << [id: errorID, time: new Date().time, message: message]
}


//
// Service Response Methods
//

private Map createResponse(args) {
    args.response = args.batch ? createSuccessResponse(args) : createErrorResponse(args)
}

private Map createSuccessResponse(args) {
    return [
        resultType: "success",
        result: args.batch
    ]
}

private Map createErrorResponse(args) {
    return [
        resultType: "error",
        result: args.errors,
    ]
}