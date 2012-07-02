package internal

import org.apache.commons.lang.exception.ExceptionUtils
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObjectService
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObject
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObjectSearchCriteria
import com.axeda.platform.sdk.v1.services.extobject.PropertySearchCriteria
import com.axeda.platform.sdk.v1.services.extobject.expression.PropertyExpressionFactory
import com.axeda.platform.sdk.v1.services.ServiceFactory
import com.axeda.drm.sdk.contact.Email
import com.axeda.drm.sdk.scripto.Request
import groovyx.net.http.AsyncHTTPBuilder
import com.axeda.platform.sdk.v1.services.data.DataAccumulatorService

/**
 * FinalizeBatch
 *
 * Finalize a batch, this involves marking the batch successful (or complete with errors) setting the total errors and
 * successes and then removing the excess Success Anchor objects.
 *
 * @note this service is meant to be executed on continually running RuleTimer.
 *
 * @author philip lombardi <plombardi@axeda.com>
 */

try {
    def serviceFactory = new ServiceFactory()
    def eoSvc = serviceFactory.extendedObjectService
    def daSvc = serviceFactory.dataAccumulatorService

    if (!Request.parameters.batchID) {
        logInfo(message: "Searching for completed batches.")

        def configuration = findBatchFrameworkConfiguration(eoSvc)
        def asyncHttp = new AsyncHTTPBuilder(
            poolSize: configuration.getPropertyByName("finalizerThreadPoolSize").value as Integer,
            uri: 'https://' + configuration.getPropertyByName("platformHost").value
        )

        // good luck cuz, this is a bit crazzzzy
        def completedBatches = findInProcessBatches(eoSvc).findAll {
            isBatchComplete(
                batchAnchor : it,
                successes   : findBatchSuccesses(queryCriteria: [id: it.id], eoSvc),
                errors      : findBatchErrors(queryCriteria: [id: it.id], eoSvc))
        }

        logInfo(message: "Finalizing (${completedBatches.size()}) batches.")

        completedBatches.sort { a, b -> a.id <=> b.id }.each { completeBatch ->
            asyncHttp.get(
                path: '/services/v1/rest/Scripto/execute/BatchFinalizer',
                query: [
                    username: configuration.getPropertyByName("platformUsername").value,
                    password: configuration.getPropertyByName("platformPassword").value,
                    batchID: completeBatch.id
                ]
            )
        }
    } else {
        logInfo(message: "Finalizing batch with ID = '${Request.parameters.batchID}'.")

        def anchor = findBatchAnchor(queryCriteria: [id: Request.parameters.batchID], eoSvc)
        def errors = findBatchErrors(queryCriteria: [id: Request.parameters.batchID], eoSvc)

        updateBatchAnchorProperty(batchAnchor: anchor, propertyName: "completionTime", propertyValue: new Date().time, eoSvc)

        notifyByEmail(
            emailTo: anchor.getPropertyByName("notifyEmail").value,
            subject: "Batch Completed",
            message: "Batch '${anchor.getPropertyByName("name").value}' has completed with (${errors.size()}) errors.")
        !errors ? markBatchSuccess(batchAnchor: anchor, eoSvc) : markBatchCompleteWithErrors(batchAnchor: anchor, eoSvc)

        deleteBatchTasks(anchor, daSvc)
    }
}
catch (any) {
    logError(message: ExceptionUtils.getFullStackTrace(any))
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

private ExtendedObject findBatchAnchor(args, ExtendedObjectService eoSvc) {
    def criteria = new ExtendedObjectSearchCriteria()
    criteria.extendedObjectClassName = "connexion.batch.Anchor"
    criteria.id = args.queryCriteria.id as Long

    def queryResult = eoSvc.findExtendedObjects(criteria, -1, -1, null)[0]
    if (!queryResult) {
        logError(message: "Unable to find batch with provided ID: '${args.queryCriteria.id}'.")
    }

    return queryResult
}

private List<ExtendedObject> findInProcessBatches(ExtendedObjectService eoSvc) {
    def propertyCriteria = new PropertySearchCriteria()
    propertyCriteria.propertyExpression = PropertyExpressionFactory.eq("state", "IN_PROCESS")

    def criteria = new ExtendedObjectSearchCriteria()
    criteria.extendedObjectClassName = "connexion.batch.Anchor"
    criteria.propertySearchCriteria = propertyCriteria

    eoSvc.findExtendedObjects(criteria, -1, -1, null)
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

private Boolean isBatchComplete(args) {
    logInfo(message: "Checking if batch with ID = '${args.batchAnchor.id}' is complete.")
    logInfo(message: "Success count = (${args.successes.size()}), Error count = (${args.errors.size()})")


    def totalTasks = args.batchAnchor.getPropertyByName("size").value as Integer
    def totalCompleted = (args.successes + args.errors).size()

    logInfo(message: "There were provided tasks = '${totalTasks}' and (${totalCompleted}) completed..")

    return totalTasks == totalCompleted
}

private void markBatchSuccess(args, ExtendedObjectService eoSvc) {
    def status = args.batchAnchor.getPropertyByName("state")
    status.value = "SUCCESS"
    eoSvc.updateProperty(status)
}

private void markBatchCompleteWithErrors(args, ExtendedObjectService eoSvc) {
    def status = args.batchAnchor.getPropertyByName("state")
    status.value = "COMPLETE_WITH_ERROR"
    eoSvc.updateProperty(status)
}

private void deleteBatchTasks(ExtendedObject batchAnchor, DataAccumulatorService daSvc) {
    if (daSvc.doesAccumulationExist("Batch", batchAnchor.id)) {
        logInfo(message: "Deleting batch tasks with ID = (${batchAnchor.id}).")
        daSvc.deleteAccumulation("Batch", batchAnchor.id)
    }
}

private void notifyByEmail(args) {
    Email.send("noreply@axeda.com", args.emailTo as String, args.subject as String, args.message as String)
}

private void updateBatchAnchorProperty(args, ExtendedObjectService eoSvc) {
    def property = args.batchAnchor.getPropertyByName(args.propertyName)
    property.value = args.propertyValue as String
    eoSvc.updateProperty(property)
}

//
// Service Utility Methods
//

private void processFatalException(args) {
    logError(message: ExceptionUtils.getFullStackTrace(args.throwable as Throwable))
}

private void logInfo(args) {
    logger.info args.message
}

private void logError(args) {
    logger.error args.message
}
