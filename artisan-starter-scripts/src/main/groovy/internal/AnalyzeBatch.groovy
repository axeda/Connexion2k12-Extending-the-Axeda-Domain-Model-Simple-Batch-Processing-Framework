package internal

import com.axeda.drm.sdk.scripto.Request
import com.axeda.platform.sdk.v1.services.ServiceFactory
import java.util.regex.Pattern
import groovy.json.JsonOutput
import org.apache.commons.lang.exception.ExceptionUtils
import com.axeda.platform.sdk.v1.services.data.DataAccumulatorService

/**
 * AnalyzeBatch
 *
 * AnalyzeBatch visits each task in a batch and validates whether it is valid or not.
 *
 * Request contract:
 *
 * A single HTTP query parameter identifying the batch (it's ID number)
 *
 * @param batchID -
 *
 * Success (all tasks are valid) Response contract:
 *
 * {
 *     "resultType": "success",
 *     "result": [
 *          "taskCount": 100,
 *     ]
 * }
 *
 * Success (some or all tasks are invalid) Response contract:
 *
 * {
 *     "resultType": "success",
 *     "result": [
 *          {
 *              "task": 1,
 *              "field": "modelName",
 *              "reason": "Model name length exceeds maximum length.",
 *          },
 *          {
 *              "task": 23,
 *              "field": "serialNumber",
 *              "reason": "Serial Number is empty or missing.",
 *          }
 *     ]
 * }
 *
 * Error Response contract:
 *
 * {
 *     "resultType": "error",
 *     "result": [
 *          {
 *              "id": "1234",
 *              "time": 0,
 *              "message": "An example error."
 *          }
 *     ]
 * }
 *
 * @author philip lombardi <plombardi@axeda.com>
 */

String mock         = Request.parameters.mock
String mockError    = Request.parameters.mockError
String contentType  = "text/plain"

Map response    = [:]
List errors     = []

//
// MAXIMUM TASK COUNT. More tasks than this will result in a fatal error.
//
final Long MAXIMUM_TASK_COUNT = 1000
final Long MAXIMUM_COMMAS = 2

//
// Task Pattern
//
// Each task should conform to the below Regular Expression.
//
// fmt: ModelName,SerialNumber,AssetName,Properties<CRLF>
// "Properties" fmt: Property1=Value1,Property2=Value2,Property3=Value3
//
// example: LCDMonitor,LCD-AX1,SuperLCD,Cadence|Brightness|
//

def taskPattern = ~/([\w]){1,100},([\w-]){1,200},([\w-]{1,200})/
def taskClosure = { matcher, modelName, serialNumber, assetName ->
    logInfo(message: "Analyzing task... Model = '${modelName}', SerialNumber = '${serialNumber}', AssetName = '${assetName}'.")
}

try {
    def serviceFactory = new ServiceFactory()
    def daSvc = serviceFactory.dataAccumulatorService

    def batchID = Request.parameters.batchID as Long
    logInfo(message: "BatchID = '${batchID}'.")

    def lineReader = new LineNumberReader(fetchBatchTasks(batchID: batchID, errors: errors, daSvc))
    def taskCount = computeTaskCount(tasks: fetchBatchTasks(batchID: batchID, errors: errors, daSvc))

    if (taskCount > MAXIMUM_TASK_COUNT) {
        createError(errors, "1030", "Number of tasks exceeds maximum allowed by service per batch. Maximum = (${MAXIMUM_TASK_COUNT}), but Provided = (${taskCount}).")
    }

    analyzeTasks(tasks: lineReader, maxCommas: MAXIMUM_COMMAS, taskPattern: taskPattern, taskClosure: taskClosure, errors: errors)

    response = createResponse(response: response, result: [taskCount: taskCount], error: errors)
}
catch (any) {
    processFatalException(throwable: any, errors: errors)
}

return createReturnMap(contentType, response)

//
// script implemenation methods
//

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

private Long computeTaskCount(args) {
    logInfo(message: "Computing number of tasks in batch.")
    def lineReader = new LineNumberReader(args.tasks as InputStreamReader)
    lineReader.skip(Long.MAX_VALUE)
    return lineReader.lineNumber + 1
}

private void analyzeTasks(args) {
    LineNumberReader tasks = args.tasks
    Pattern taskPattern = args.taskPattern

    tasks.eachLine { String task, lineNumber ->
        if (task.count(/,/) > args.maxCommas) {
            createError(args.errors, "1031", "Task at line: (${lineNumber}) contains too many ',' (comma) characters. Maximum allowed is (${args.maxCommas}). Individual fields CAN NOT contain ANY commas.")
        } else if(!task.matches(taskPattern)) {
            createError(args.errors, "1031", "Task at line: (${lineNumber}) has invalid form. Check documentation for the correct format.")
        }
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
