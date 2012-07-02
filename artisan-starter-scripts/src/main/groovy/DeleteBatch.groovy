import com.axeda.drm.sdk.scripto.Request
import com.axeda.platform.sdk.v1.services.ServiceFactory
import groovy.json.JsonOutput
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObject
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObjectService
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObjectSearchCriteria
import com.axeda.platform.sdk.v1.services.data.DataAccumulatorService
import groovy.json.JsonSlurper
import org.apache.commons.lang.exception.ExceptionUtils
import java.beans.PropertyChangeListener

/**
 * DeleteBatch
 *
 * Remove a batch from the processing queue. This service can only remove batches that are still in the state:
 * 'NOT_STARTED'. Any attempt to stop a Batch in another state will result in an error.
 *
 * Request Contract:
 *
 * {
 *      "criteria": { "id": "102289" }
 * }
 *
 * Success Response Contract:
 *
 * {
 *      "resultType": "success",
 *      "result": true
 * }
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
    criteria: [
        id: "102289"
    ]
]

Map mockResponse = [
    resultType: "success",
    result: true
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

Map response    = [:]
List errors     = []
List infos      = []

try {
    //
    // mock switches
    //
    if (mock)       { return createReturnMap(contentType, mockResponse) }
    if (mockError)  { return createReturnMap(contentType, mockErrorResponse) }

    //
    // service request validation
    //
    def request = new JsonSlurper().parseText(Request.body) as Map
    if (!isValidRequest(request: request, errors: errors)) {
        return createReturnMap(contentType, createErrorResponse(response: response, errors: errors))
    }

    //
    // service implementation
    //
    def serviceFactory = new ServiceFactory()
    def daSvc = serviceFactory.dataAccumulatorService
    def eoSvc = serviceFactory.extendedObjectService

    // grab the batch requested.
    def batchAnchor = findBatchAnchor(queryCriteria: request.criteria, eoSvc)

    if (batchAnchor) {
        def batchState = batchAnchor?.getPropertyByName("state")?.value

        if (batchState == "NOT_STARTED") {
            deleteBatchAnchor(batchAnchor, eoSvc)
            deleteBatchTasks(batchAnchor, daSvc)
        } else {
            createError(errors, "1020", "Unable to delete batch because the batch is an unacceptable state for deletion: '${batchState}'.")
        }
    } else {
        createError(errors, "1002", "Unable to delete batch because the batch with ID = '${request.criteria.id}' could not be found.")
    }

    response = createResponse(success: true, errors: errors)
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

private void deleteBatchAnchor(ExtendedObject batchAnchor, ExtendedObjectService eoSvc) {
    eoSvc.deleteExtendedObject(batchAnchor.id)
}

private void deleteBatchTasks(ExtendedObject batchAnchor, DataAccumulatorService daSvc) {
    if (daSvc.doesAccumulationExist("BatchTasks", batchAnchor.id)) {
        daSvc.deleteAccumulation("BatchTasks", batchAnchor.id)
    }
}

//
// Service Utility Methods
//

private Map createReturnMap(String contentType, output) {
    return ['Content-Type': contentType, Content: JsonOutput.toJson(output)]
}

private Map processFatalException(args) {
    logError(message: ExceptionUtils.getFullStackTrace(args.throwable as Throwable))
    createError(args.errors, "0001", "Fatal execution exception. Check logs or contact your system administrator.")
}

private void createError(List errors, String errorID, String message) {
    errors << [id: errorID, time: new Date().time, message: message]
}

private void createInfo(List infos, String message) {
    infos << [time: new Date().time, message: message]
}

private void logInfo(args) {
    logger.info args.message
}

private void logError(args) {
    logger.error args.message
}

//
// Service Response Methods
//

private Map createResponse(args) {
    return !args.errors ? createSuccessResponse(args) : createErrorResponse(args)
}

private Map createSuccessResponse(args) {
    return [
        resultType: "success",
        pageInfo: args.pageInfo,
        result: args.result
    ]
}

private Map createErrorResponse(args) {
    return [
        resultType: "error",
        result: args.errors,
    ]
}