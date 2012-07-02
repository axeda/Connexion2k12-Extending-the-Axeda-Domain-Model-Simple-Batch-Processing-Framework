import com.axeda.platform.sdk.v1.services.extobject.ExtendedObject
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObjectService
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObjectSearchCriteria
import groovy.json.JsonOutput
import com.axeda.drm.sdk.scripto.Request
import groovy.json.JsonSlurper
import com.axeda.platform.sdk.v1.services.ServiceFactory
import com.axeda.platform.sdk.v1.services.extobject.PropertySearchCriteria
import com.axeda.platform.sdk.v1.services.extobject.expression.PropertyExpressionFactory
import org.apache.commons.lang.exception.ExceptionUtils
import net.sf.json.JSONObject

/**
 * GetBatches
 *
 * GetBatches returns a list of Batches
 *
 * Request Contract:
 *
 * {
 *     "criteria": {
 *         "page": 1,
 *         "pageSize": 10,
 *     }
 * }
 *
 * Optionally, one can specify only to search for a specific Batch state.
 *
 * {
 *     "criteria": {
 *         "page": 1,
 *         "pageSize": 10,
 *         "state": "IN_PROCESS"
 *     }
 * }
 *
 * Success Response Contract:
 *
 * {
 *     "resultType": "success",
 *     "result": [
 *          {
 *              "id": 1336,
 *              "name": "Example Batch 1",
 *              "inception": 0,
 *              "state": "IN_PROCESS",
 *          },
 *          {
 *              "id": 1337,
 *              "name": "Example Batch 2",
 *              "inception": 0,
 *              "state": "NOT_STARTED",
 *          },
 *          {
 *              "id": 1338,
 *              "name": "Example Batch",
 *              "inception": 0,
 *              "state": "COMPLETE",
 *          }
 *     ]
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
    criteria: [
        pageNumber: 1,
        pageSize: 10,
        state: "NOT_STARTED"
    ]
]

Map mockResponse = [
    resultType: "success",
    pageInfo:[
        pageNumber      : 1,
        pageSize        : 10,
        totalResults    : 1
    ],
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
    def requestBody = new JsonSlurper().parseText(Request.body) as Map

    //
    // service implementation logic.
    //
    def eoSvc = new ServiceFactory().extendedObjectService
    def criteria = requestBody.criteria

    def batchAnchors = findBatchAnchors(queryCriteria: criteria, errors: errors, eoSvc)
    def totalResults = batchAnchors.size()

    batchAnchors = fauxPaginate(
        batchAnchors.sort { a, b -> a.id <=> b.id }, criteria.pageSize, criteria.pageNumber).collect { createBatchMap(it as ExtendedObject) }

    response = createResponse(pageInfo: createPageInfo(criteria, totalResults), result: batchAnchors, errors: errors)
}
catch (any) {
    processFatalException(throwable: any, response: response)
}

return createReturnMap(contentType, response)

//
// Service Implementation Methods
//
private Boolean isValidRequest(args) {
    true
}

private List<ExtendedObject> findBatchAnchors(args, ExtendedObjectService eoSvc) {
    def criteria = new ExtendedObjectSearchCriteria()
    criteria.extendedObjectClassName = "connexion.batch.Anchor"

    if (args.queryCriteria.state) {
        def propertyCriteria = new PropertySearchCriteria()
        propertyCriteria.propertyExpression = PropertyExpressionFactory.eq("state", args.queryCriteria.state)
        criteria.propertySearchCriteria = propertyCriteria
    }

    def queryResult = eoSvc.findExtendedObjects(criteria, -1, -1, null)

    return queryResult
}

private List fauxPaginate(List objects, Integer pageSize, Integer pageNumber) {
    if (!objects) { return [] }

    page = pageNumber > 0 ? pageNumber : 1
    pageSize = pageSize > 0 ? pageSize : 1

    def endIndex = page * pageSize
    def beginIndex = endIndex - pageSize

    return objects[beginIndex..<(endIndex < objects.size() ? endIndex : objects.size())]
}

private Map createPageInfo(Map criteria, Integer totalResults) {
    return [pageNumber: criteria.pageNumber, pageSize: criteria.pageSize, totalResults: totalResults]
}

private Map createBatchMap(ExtendedObject batchAnchor) {

    if (batchAnchor) {
        return [
            id: batchAnchor.id,
            name: batchAnchor.getPropertyByName("name").value,
            inceptionTime: batchAnchor.getPropertyByName("inceptionTime")?.value,
            completionTime: batchAnchor.getPropertyByName("completionTime")?.value,
            size: batchAnchor.getPropertyByName("size").value as Integer,
            state: batchAnchor.getPropertyByName("state").value,
            notifyEmail: batchAnchor.getPropertyByName("notifyEmail").value,
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
