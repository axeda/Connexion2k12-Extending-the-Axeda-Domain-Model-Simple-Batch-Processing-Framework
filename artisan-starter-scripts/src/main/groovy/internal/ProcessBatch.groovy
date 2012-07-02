package internal

import com.axeda.drm.sdk.device.Model
import com.axeda.drm.sdk.device.Device
import com.axeda.drm.sdk.scripto.Request
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObjectService
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObject
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObjectSearchCriteria
import groovy.json.JsonOutput
import org.apache.commons.lang.exception.ExceptionUtils
import com.axeda.platform.sdk.v1.services.ServiceFactory
import com.axeda.drm.sdk.Context
import com.axeda.platform.sdk.v1.common.exception.PlatformSDKException
import java.util.regex.Pattern
import com.axeda.drm.sdk.device.DevicePropertyFinder
import com.axeda.drm.sdk.device.ModelFinder
import com.axeda.drm.sdk.device.DeviceFinder
import com.axeda.platform.sdk.v1.services.extobject.Property
import com.axeda.services.v2.ExtendedList
import com.axeda.sdk.v2.dsl.Bridges
import com.axeda.platform.sdk.v1.services.data.DataAccumulatorService
import com.axeda.platform.sdk.v1.common.exception.EntityAlreadyExistsException

/**
 * ProcessBatch
 *
 * ProcessBatch visits each task in a batch and validates whether it is valid or not.
 *
 * Request contract:
 *
 * A single HTTP query parameter identifying the batch (it's ID number)
 *
 * @param batchID -
 *
 * Success (all tasks are valid, and execution completes with no errors) Response contract:
 *
 * {
 *     "resultType": "success",
 *     "result": true,
 * }
 *
 * Error Response contract:
 *
 * {
 *      "resultType": "success",
 *      "result": true,
 * }
 *
 * @author philip lombardi <plombardi@axeda.com>
 */

String contentType  = "text/plain"

try {
    logInfo(message: "Processing batch with ID = '${Request.parameters.batchID}'")

    def serviceFactory = new ServiceFactory()
    def eoSvc = serviceFactory.extendedObjectService
    def daSvc = serviceFactory.dataAccumulatorService

    def batchID = Request.parameters.batchID
    def batchAnchor = findBatchAnchor(queryCriteria: [id: batchID], eoSvc)
    def errorReasons = []

    if (batchAnchor) {
        markBatchInProgress(batchAnchor: batchAnchor, eoSvc)

        def lineReader = new LineNumberReader(fetchBatchTasks(batchID: batchID, errors: errorReasons, daSvc))
        processTasks(tasks: lineReader, batchAnchor: batchAnchor, eoSvc)
    }
}
catch (any) {
    logError(message: ExceptionUtils.getFullStackTrace(any))
}

createReturnMap(contentType, JsonOutput.toJson([resultType: 'success', result: true]))

//
// script implementation methods
//

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

private InputStreamReader fetchBatchTasks(args, DataAccumulatorService daSvc) {
    logInfo(message: "Retrieving batch tasks for batch ID = '${args.batchID}' ... ")
    if (daSvc.doesAccumulationExist("BatchTasks", args.batchID as Long)) {
        logInfo(message: "Data accumulation exists, wrapping in a stream reader.")
        return new InputStreamReader(daSvc.streamAccumulation("BatchTasks", args.batchID as Long))
    } else {
        createError(args.errors, "1010", "Unable to find a Batch with ID: '${args.batchID}'.")
        return null
    }
}

private void processTasks(args, eoSvc) {
    LineNumberReader tasks = args.tasks
    Context context = Context.getSDKContext()

    tasks.eachLine { String task, lineNumber ->
        List errorReasons = []

        def (modelNumber, serialNumber, assetName) = task.split(/,/)
        logInfo(message: "Attempting to create model = '${modelNumber}', serial number = '${serialNumber}', asset name = '${assetName}'")

        createModel(modelNumber: modelNumber, errors: errorReasons)

        def model = findModel(modelNumber: modelNumber, new ModelFinder(context))

        createAsset(model: model, serialnumber: serialNumber, name: assetName, errors: errorReasons)

        if (!errorReasons) {
            createBatchSuccess(batchID: args.batchAnchor.id, inceptionTime: new Date().time, eoSvc)
        } else {
            String listID = "BER:${args.batchAnchor.id}:${lineNumber}"

            logInfo(message: "Storing error reasons with ID = '${listID}'.")

            createBatchErrorMessagesList(listID: listID, reasons: errorReasons)
            createBatchError(
                batchID         : args.batchAnchor.id,
                taskNumber      : lineNumber,
                messagesListID  : listID,
                inceptionTime   : new Date().time,
                eoSvc)
        }
    }
}

private void createModel(args) {
    logInfo(message: "Creating model: '${args.modelNumber}'")
    def model = new Model(Context.getSDKContext(), args.modelNumber as String)
    try {
        model.store()
    } catch (any) {
        logError(message: any.localizedMessage)

        // kind of a silly error to report.
        //args.errors << "Could not create model with model number = '${args.modelNumber}'."
    }
}

private Model findModel(args, ModelFinder finder) {
    finder.setName(args.modelNumber as String)
    finder.find()
}

private Device findAsset(args, DeviceFinder finder) {
    finder.model = args.model
    finder.setSerialNumber(args.serialNumber as String)
    finder.find()
}

private void findModelProperties(args, DevicePropertyFinder propertyFinder) { }

private void findAssetProperties(args) { }

private void createOrUpdateModelProperties(args) { }

private void setAssetProperties(args) { }

private void createAsset(args) {
    logInfo(message: "Creating asset: '${args.model?.name}||${args.serialnumber}'")
    def asset = new Device(Context.getSDKContext(), args.serialnumber, args.model)
    asset.name = args.name

    try {
        asset.store()
    } catch (any) {
        logError(message: any.localizedMessage)
        args.errors << "Asset with model number = '${args.model?.name}' and serial number = '${args.serialnumber}' could not be created."
    }
}

private void markBatchInProgress(args, ExtendedObjectService eoSvc) {
    logInfo(message: "Marking batch as: 'IN_PROCESS'")
    def status = args.batchAnchor.getPropertyByName("state")
    status.value = "IN_PROCESS"
    eoSvc.updateProperty(status)
}

private ExtendedObject createExtendedObject(args, ExtendedObjectService eoSvc) {
    def type = eoSvc.findExtendedObjectTypeByClassname(args.className)

    if (type) {
        logInfo(message: "Creating extended object of type: '${args.className}'")
        def object = new ExtendedObject()
        object.extendedObjectType   = type
        object.internalObjectId     = args.referenceID
        object.externalClientKey    = args.referenceName

        args.properties.each { String name, value ->
            def property = new Property()
            property.propertyType = type.getPropertyTypeByName(name)
            property.value = value as String

            object.addProperty(property)
        }

        try {
            return eoSvc.createExtendedObject(object)
        } catch (PlatformSDKException psdke) {
            logError(message: psdke.message)
            return null
        }
    } else {
        logError(message: "Extended Object Type with className: '${args.className}' does not exist.")
        return null
    }
}

private void createBatchSuccess(args, ExtendedObjectService eoSvc) {
    def object = createExtendedObject(
        className       : 'connexion.batch.SuccessAnchor',
        referenceID     : args.batchID,
        referenceName   : 'Batch Success',
        properties      : [
            inceptionTime: args.inceptionTime,
        ],
        eoSvc
    )
}

private void createBatchError(args, ExtendedObjectService eoSvc) {
    createExtendedObject(
        className       : 'connexion.batch.ErrorAnchor',
        referenceID     : args.batchID,
        referenceName   : 'Batch Error',
        properties      : [
            taskNumber      : args.taskNumber,
            messagesListID  : args.messagesListID,
            inceptionTime   : args.inceptionTime,
        ],
        eoSvc
    )
}

private void createBatchErrorMessagesList(args) {
    logInfo(message: "Storing (${args.reasons.size()}) messages in list: '${args.listID}'.")

    if (args.reasons) { logInfo(message: "Reasons = ${args.reasons}") }

    def list = new ExtendedList()
    list.name = args.listID
    args.reasons.each { Bridges.extendedListBridge.append(args.listID, it) } // bug in 6.1.6 xlist, cant assign to list?

    //Bridges.extendedListBridge.create(list)
}

//
// script Utility Methods
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
