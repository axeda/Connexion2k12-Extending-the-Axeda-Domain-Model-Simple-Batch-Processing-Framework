package internal

import groovyx.net.http.AsyncHTTPBuilder
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObject
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObjectService
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObjectSearchCriteria
import com.axeda.platform.sdk.v1.services.extobject.PropertySearchCriteria
import com.axeda.platform.sdk.v1.services.extobject.expression.PropertyExpressionFactory
import com.axeda.platform.sdk.v1.services.ServiceFactory
import groovy.json.JsonOutput
import org.apache.commons.lang.exception.ExceptionUtils
import groovyx.net.http.RESTClient

/**
 * BatchIgniter
 *
 * This service is meant to be run on a rule timer. It async invokes ProcessBatch via an AsyncHttpBuilder object
 * which then handles the processing of a batch.
 *
 * @note To avoid the rule sniper picking off this rule. Regardless of how many NOT_STARTED Batch jobs are queued for
 * processing, this service will never start the processor for more jobs than the property
 * igniterThreadPoolSize on the Batch Framework Configuration object.
 *
 * @note Ignition is ordered by the 'inceptionTime' value on the Batch Anchor. Lower value inceptions start before higher
 * value inceptions.
 *
 * @author philip lombardi <plombardi@axeda.com>
 */

List errors = []

try {
    def eoSvc = new ServiceFactory().extendedObjectService

    // grab the framework configuration
    def configuration = findConfiguration(eoSvc)

    if (configuration) {
        def threadPoolSize = configuration.getPropertyByName("igniterThreadPoolSize").value as Integer
        def platformHost = configuration.getPropertyByName("platformHost").value
        def platformUsername = configuration.getPropertyByName("platformUsername").value
        def platformPassword = configuration.getPropertyByName("platformPassword").value

        def igniter = new AsyncHTTPBuilder(
            poolSize: threadPoolSize,
            uri: 'https://' + platformHost,
        )

        //def igniter = new RESTClient("https://" + platformHost)

        // get all NOT_STARTED batches.
        logInfo(message: "Searching for 'NOT_STARTED' batches.")
        def batchAnchors = findBatchAnchors(queryCriteria: [state: "NOT_STARTED"], eoSvc).sort { a, b ->
            a.getPropertyByName("inceptionTime")?.value?.toLong() <=> b.getPropertyByName("inceptionTime")?.value?.toLong()
        }

        // pick N < THREAD_POOL_SIZE batches based on lowest to highest inception to begin processing.
        def batchesToProcess = batchAnchors[0..<(threadPoolSize < batchAnchors.size() ? threadPoolSize : batchAnchors.size())]

        logInfo(message: "Igniting (${batchesToProcess.size()}) batches.")

        // start ignitions.
        batchesToProcess.each {
            logInfo(message: "Igniting batch with ID = '${it.id}'.")
            igniter.get(
                path: '/services/v1/rest/Scripto/execute/ProcessBatch',
                query: [username: platformUsername, password: platformPassword, batchID: it.id]
            )
        }

    } else {
        logError(message: "Unable to find connexion.batch.Configuration object.")
    }
}
catch (any) {
    processFatalException(throwable: any, errors: errors)
}

//
// Script Implementation Methods
//
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

private ExtendedObject findConfiguration(ExtendedObjectService eoSvc) {
    def criteria = new ExtendedObjectSearchCriteria()
    criteria.extendedObjectClassName = "connexion.batch.Configuration"
    eoSvc.findExtendedObjects(criteria, -1, -1, null)[0]
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
    args.response = !args.errors ? createSuccessResponse(args) : createErrorResponse(args)
}

private Map createSuccessResponse(args) {
    return [
        resultType: "success",
        result: args.result
    ]
}

private Map createErrorResponse(args) {
    return [
        resultType: "error",
        result: args.errors,
    ]
}