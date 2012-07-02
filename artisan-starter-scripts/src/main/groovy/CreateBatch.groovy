import com.axeda.drm.sdk.scripto.Request
import groovy.json.JsonOutput
import com.axeda.platform.sdk.v1.services.ServiceFactory
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObjectService
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObject
import com.axeda.platform.sdk.v1.services.data.DataAccumulatorService
import com.axeda.platform.sdk.v1.services.extobject.Property
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObjectSearchCriteria
import groovyx.net.http.RESTClient
import groovy.json.JsonSlurper
import org.apache.commons.lang.exception.ExceptionUtils

/**
 * CreateBatch
 *
 * Request Contract:
 *
 * The request contract is an upload attachment of CSV data in the following format:
 *
 * ModelName,SerialNumber,AssetName,Property1=Value1;Property2=Value2;...PropertyN=ValueN;,AssetGroup
 *
 * An optional HTTP query parameter can also be passed along:
 *
 * @param batchName: String [optional] - a name to give the batch.
 * @param notifyEmail: String [optional] - an email address to notify upon batch processing completion.
 *
 * Success Response Contract:
 *
 * {
 *     "resultType": "success",
 *     "result": true
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
Map mockRequest = [:]

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

// default batch name to use if one is not provided.
String DEFAULT_BATCH_NAME = "Connexion 2012 Batch Processing Job"

try {
    logInfo(message: "CreateBatch has been invoked.")

    //
    // mock switches
    //
    if (mock)       { return createReturnMap(contentType, mockResponse) }
    if (mockError)  { return createReturnMap(contentType, mockErrorResponse) }

    //
    // service request validation
    //

    //
    // service implementation
    //
    logInfo(message: "Initializing Platform services.")
    def serviceFactory = new ServiceFactory()
    def daSvc = serviceFactory.dataAccumulatorService
    def eoSvc = serviceFactory.extendedObjectService

    // grab the framework configuration object.
    logInfo(message: "Retrieving Batch Framework Configuration.")
    def configuration = findBatchFrameworkConfiguration(eoSvc)

    if (configuration) {
        def batchAnchor = createBatchAnchor(name: Request.parameters.batchName ?: DEFAULT_BATCH_NAME, eoSvc)
        createBatchTasks(batchID: batchAnchor.id, tasks: Request.body, daSvc)

        def platformHost = configuration.getPropertyByName("platformHost").value
        logInfo(message: "platform host = ${platformHost}")

        // call validation service
        def restClient = new RESTClient('https://' + platformHost)
        def analyzerResult = restClient.get(
            path: '/services/v1/rest/Scripto/execute/AnalyzeBatch',
            query: [
                username: configuration.getPropertyByName("platformUsername")?.value,
                password: configuration.getPropertyByName("platformPassword")?.value,
                batchID: batchAnchor.id as String
            ])

        analyzerResult = new JsonSlurper().parse(analyzerResult.data) as Map

        logInfo(message: "Raw return from analyzer = ${analyzerResult}")

        // extract analyzer result data
        Boolean success = false
        if (analyzerResult.resultType == "success") {
            updateBatchAnchorProperty(batchAnchor: batchAnchor, propertyName: 'size', propertyValue: analyzerResult.result.taskCount, eoSvc)
            updateBatchAnchorProperty(batchAnchor: batchAnchor, propertyName: 'notifyEmail', propertyValue: Request.parameters.notifyEmail as String, eoSvc)
            success = true
        } else {
            errors += analyzerResult.result

            // cleanup for failure.
            deleteBatchAnchor(batchAnchor, eoSvc)
            deleteBatchTasks(batchAnchor, daSvc)
        }
    } else {
        logError(message: "Unable to find connexion.batch.Configuration object.")
        createError(errors, "1001", "Unable to find connexion.batch.Configuration object.")
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
private ExtendedObject createBatchAnchor(args, ExtendedObjectService eoSvc) {
    logInfo(message: "Creating batch anchor with name: '${args.name}'.")

    def batchAnchorType = eoSvc.findExtendedObjectTypeByClassname("connexion.batch.Anchor")

    def batchAnchor = new ExtendedObject()
    batchAnchor.externalClientKey = args.name
    batchAnchor.extendedObjectType = batchAnchorType

    [
        name            : args.name,
        size            : 0,
        completionTime  : 0,
        inceptionTime   : new Date().time as String,
        state           : "NOT_STARTED",
        notifyEmail     : "blackhole@example.com",
    ].each { String propertyName, value ->
        def property = new Property()
        property.propertyType = batchAnchorType.getPropertyTypeByName(propertyName)
        property.value = value as String
        batchAnchor.addProperty(property)
    }

    batchAnchor = eoSvc.createExtendedObject(batchAnchor)

    // this appears to be a timing bug...
    def criteria = new ExtendedObjectSearchCriteria()
    criteria.id = batchAnchor.id
    batchAnchor = eoSvc.findExtendedObjects(criteria, -1, -1, null)[0]

    logInfo(message: "Created a batch anchor with name: '${batchAnchor.getPropertyByName("name").value}' and ID = (${batchAnchor.id}) ")
    return batchAnchor
}

private void updateBatchAnchorProperty(args, ExtendedObjectService eoSvc) {
    def property = args.batchAnchor.getPropertyByName(args.propertyName)
    property.value = args.propertyValue as String
    eoSvc.updateProperty(property)
}

private void createBatchTasks(args, DataAccumulatorService daSvc) {
    logInfo(message: "Writing batch tasks to data accumulation.")
    daSvc.writeChunk("BatchTasks", args.batchID as Long, args.tasks)
}

private void deleteBatchAnchor(ExtendedObject batchAnchor, ExtendedObjectService eoSvc) {
    logInfo(message: "Deleting batch anchor with ID = (${batchAnchor.id}).")
    eoSvc.deleteExtendedObject(batchAnchor.id)
}

private void deleteBatchTasks(ExtendedObject batchAnchor, DataAccumulatorService daSvc) {
    if (daSvc.doesAccumulationExist("Batch", batchAnchor.id)) {
        logInfo(message: "Deleting batch tasks with ID = (${batchAnchor.id}).")
        daSvc.deleteAccumulation("Batch", batchAnchor.id)
    }
}

private ExtendedObject findBatchFrameworkConfiguration(ExtendedObjectService eoSvc) {
    def criteria = new ExtendedObjectSearchCriteria()
    criteria.extendedObjectClassName = "connexion.batch.Configuration"

    def result = eoSvc.findExtendedObjects(criteria, -1, -1, null)[0]

    if (result) {
        return result
    } else {
        throw new Exception("Unable to find batch processing framework configuration.")
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
        result: args.success
    ]
}

private Map createErrorResponse(args) {
    return [
        resultType: "error",
        result: args.errors,
    ]
}