import com.axeda.platform.sdk.v1.services.extobject.ExtendedObjectService
import groovy.json.JsonOutput
import com.axeda.platform.sdk.v1.services.extobject.PropertyDataType
import com.axeda.drm.sdk.scripto.Request
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObjectType
import com.axeda.platform.sdk.v1.services.extobject.PropertyType
import com.axeda.platform.sdk.v1.services.extobject.ExtendedObject
import com.axeda.platform.sdk.v1.services.extobject.Property
import com.axeda.platform.sdk.v1.services.ServiceFactory
import groovy.json.JsonSlurper
import org.apache.commons.lang.exception.ExceptionUtils
import com.axeda.platform.sdk.v1.common.exception.PlatformSDKException
import com.axeda.drm.sdk.rules.engine.ExpressionRule
import com.axeda.drm.sdk.Context
import com.axeda.drm.sdk.rules.ruletimers.RuleTimer
import com.axeda.drm.sdk.rules.engine.ExpressionRuleFinder
import com.axeda.drm.sdk.rules.engine.Expression
import com.axeda.drm.sdk.rules.ruletimers.RuleTimerFinder
import java.beans.PropertyChangeListener

/**
 * InitializeBatchFramework
 *
 * Request Contract:
 *
 * {
 *     "options": {
 *          "forceCreate": true
 *     },
 *     "platformHost": "subdomain.domain.tld",
 *     "platformUsername": "USERNAME",
 *     "platformPassword": "PASSWORD",
 *     "igniterThreadPoolSize": 2,
 *     "finalizerThreadPoolSize": 2,
 *     "igniterSchedule": "0 0/1 * * * ?",
 *     "finalizerSchedule": "0 0/1 * * * ?"
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
    options: [
        forceCreate: true,
    ],
    platformHost: "SUBDOMAIN.DOMAIN.TLD",
    platformUsername: "USERNAME",
    platformPassword: "PASSWORD",
    igniterThreadPoolSize: 2,
    finalizerThreadPoolSize: 2
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
            id: "1234",
            time: new Date().time,
            reason: "An example error."
        ]
    ]
]


Map response    = [:]
List errors     = []
List infos      = []

//
// Configuration Options
//

try {
    //
    // mock switches
    //
    if (mock) { createReturnMap(contentType, mockResponse) }
    if (mockError) { createReturnMap(contentType, mockErrorResponse) }

    //
    // service validation
    //
    def request = new JsonSlurper().parseText(Request.body) as Map
    if (!isValidRequest(request: request, errors: errors)) {
        return createReturnMap(contentType, createErrorResponse(response: response, errors: errors))
    }

    //
    // service implementation
    //
    def eoSvc = new ServiceFactory().extendedObjectService

    //
    // create framework extended object types.
    //
    createConfigurationObjectType([forceCreate: request.options?.forceCreate], eoSvc)
    createBatchAnchorObjectType([forceCreate: request.options?.forceCreate], eoSvc)
    createBatchErrorObjectType([forceCreate: request.options?.forceCreate], eoSvc)
    createBatchSuccessObjectType([forceCreate: request.options?.forceCreate], eoSvc)

    //
    // configure the configuration object.
    //
    request.remove("options")
    createConfiguration(className: "connexion.batch.Configuration", properties: request, eoSvc)

    //
    // create the igniter expression rule and rule timer trigger
    //
    scheduleIgniter(request.igniterSchedule, true)
    scheduleFinalizer(request.finalizerSchedule, true)
}
catch (any) {
    processFatalException(throwable: any, errors: errors)
}
finally {
    response = createResponse(response: response, result: true, error: errors)
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

    request.platformHost            ?: createError(errors, "1001", "Missing request field 'platformHost'.")
    request.platformUsername        ?: createError(errors, "1001", "Missing request field 'platformUsername'.")
    request.platformPassword        ?: createError(errors, "1001", "Missing request field 'platformPassword'.")
    request.igniterThreadPoolSize   ?: createError(errors, "1001", "Missing request field 'igniterThreadPoolSize'.")
    request.finalizerThreadPoolSize ?: createError(errors, "1001", "Missing request field 'finalizerThreadPoolSize'.")

    request.igniterThreadPoolSize > 0   ?: createError(errors, "1001", "'igniterThreadPoolSize' must be have a size greater than: (${request.igniterThreadPoolSize}).")
    request.finalizerThreadPoolSize > 0 ?: createError(errors, "1001", "'finalizerThreadPoolSize' must have a size greater than: (${request.finalizerThreadPoolSize}).")

    return result
}

private void createExtendedObjectType(args, ExtendedObjectService eoSvc) {
    def eoType = null

    // covering for an annoying extendedobject behavior involving querying for a non-existent object.
    try {
        eoType = eoSvc.findExtendedObjectTypeByClassname(args.className)
    }
    catch (PlatformSDKException psdke) {
        eoType = null
    }

    if (args.forceCreate) {
        if (eoType) {
            eoSvc.deleteExtendedObjectType(eoType.id)
            eoType = null
        }

        eoType = new ExtendedObjectType()
        eoType.className = args.className
        eoType.displayName = args.displayName
        eoType.description = args.description

        args.propertyTypes.each { String name, Map properties ->
            def propertyType = new PropertyType()
            propertyType.name = name
            propertyType.dataType = properties.type
            propertyType.description = properties.description
            eoType.addPropertyType(propertyType)
        }

        eoSvc.createExtendedObjectType(eoType)
    }
}

private void createExtendedObject(args, ExtendedObjectService eoSvc) {
    def eo = new ExtendedObject()

    // covering for an annoying extendedobject behavior involving querying for a non-existent object.
    def eoType = null
    try {
        eoType = eoSvc.findExtendedObjectTypeByClassname(args.className)
    }
    catch (PlatformSDKException psdke ) {
        eoType = null
    }

    if (eoType) {
        eo.extendedObjectType = eoType
        eo.externalClientKey = args.externalClientKey

        eo = eoSvc.createExtendedObject(eo)

        args.properties.each { String name, value ->
            def property = new Property()
            property.propertyType = eoType.getPropertyTypeByName(name)
            property.value = value as String
            eo.addProperty(property)
        }

        eoSvc.updateExtendedObject(eo)
    }
}

private void createConfigurationObjectType(args, ExtendedObjectService eoSvc) {
    createExtendedObjectType(
        className: "connexion.batch.Configuration",
        displayName: "Batch Configuration",
        description: "Configuration Object for the Connexion Batch Processing Framework.",
        propertyTypes: [
            igniterThreadPoolSize: [
                description: "Number of threads the Igniter can be allocated in a single run.",
                type: PropertyDataType.Integer
            ],
            igniterSchedule: [
                description: "The CRON expression used to trigger the batch Igniter.",
                type: PropertyDataType.String
            ],
            finalizerThreadPoolSize: [
                description: "Number of threads the Finalizer can be allocated in a single run.",
                type: PropertyDataType.Integer
            ],
            finalizerSchedule: [
                description: "The CRON expression used to trigger the batch Finalizer.",
                type: PropertyDataType.String
            ],
            platformHost: [
                description: "Platform Host",
                type: PropertyDataType.String
            ],
            platformUsername: [
                description: "Platform Username",
                type: PropertyDataType.String
            ],
            platformPassword: [
                description: "Platform Password",
                type: PropertyDataType.String
            ],
        ], forceCreate: args.forceCreate, eoSvc
    )
}

private void createBatchAnchorObjectType(args, ExtendedObjectService eoSvc) {
    createExtendedObjectType(
        className: "connexion.batch.Anchor",
        displayName: "Batch Anchor",
        description: "A batch anchor stores information about a batch.",
        propertyTypes: [
            name: [
                description: "Optional, non-unique name of the batch.",
                type: PropertyDataType.String
            ],
            state: [
                description: "State of the batch.",
                type: PropertyDataType.String
            ],
            inceptionTime: [
                description: "Time of creation in milliseconds since UNIX epoch.",
                type: PropertyDataType.Integer
            ],
            completionTime: [
                description: "Time of completition in milliseconds since UNIX epoch.",
                type: PropertyDataType.Integer
            ],
            size: [
                description: "Number of tasks in the batch.",
                type: PropertyDataType.Integer,
            ],
            notifyEmail: [
                description: "An email address to send a notification too when the batch is complete.",
                type: PropertyDataType.String
            ]
        ], forceCreate: args.forceCreate, eoSvc
    )
}

private void createBatchErrorObjectType(args, ExtendedObjectService eoSvc) {
    createExtendedObjectType(
        className: "connexion.batch.ErrorAnchor",
        displayName: "Batch Error Anchor",
        description: "A batch error anchor stores information about a batch task processing error, and where more information can be found.",
        propertyTypes: [
            inceptionTime: [
                description: "Time of creation in milliseconds since UNIX epoch.",
                type: PropertyDataType.Integer
            ],
            taskNumber: [
                description: "The task number in the batch processing job that caused the failure.",
                type: PropertyDataType.Integer,
            ],
            messagesListID: [
                description: "ID pointing to the ExtendedList containing messages related to this Batch Error.",
                type: PropertyDataType.String
            ]
        ], forceCreate: args.forceCreate, eoSvc
    )
}

private void createBatchSuccessObjectType(args, ExtendedObjectService eoSvc) {
    createExtendedObjectType(
        className: "connexion.batch.SuccessAnchor",
        displayName: "Batch Success Anchor",
        description: "A batch success anchor is a temporary anchor that is created to store information about a completed batch task.",
        propertyTypes: [
            inceptionTime: [
                description: "Time of creation in milliseconds since UNIX epoch.",
                type: PropertyDataType.Integer
            ]
        ], forceCreate: args.forceCreate, eoSvc
    )
}

private void createConfiguration(args, ExtendedObjectService eoSvc) {
    createExtendedObject(
        className: args.className, externalClientKey: "Batch Framework Configuration", properties: args.properties, eoSvc)
}

private void createExpressionRule(args) {
    def expressionRule = new ExpressionRule(Context.getSDKContext())
    expressionRule.name             = args.name
    expressionRule.description      = args.description
    expressionRule.ifExpression     = args.ifExpression
    expressionRule.thenExpression   = args.thenExpression
    expressionRule.elseExpression   = args.elseExpression
    expressionRule.triggerName      = "SystemTimer"
    expressionRule.enabled          = args.enabled
    expressionRule.store()
}

private ExpressionRule findExpressionRule(args, ExpressionRuleFinder finder) {
    finder.setName(args.name)
    return finder.find()
}

def void createRuleTimer(args) {
    def ruleTimer = new RuleTimer(Context.getSDKContext())

    ruleTimer.name = args.name
    ruleTimer.description = args.description
    ruleTimer.schedule = args.cronSchedule

    args.rules.each { ruleTimer.addRule(it.id.value as Long, false) }

    ruleTimer.store()
}

private RuleTimer findRuleTimer(args, RuleTimerFinder finder) {
    finder.setName(args.name)
    finder.findOne() as RuleTimer
}

private void scheduleIgniter(String cronSchedule, Boolean force) {
    String igniterName = "connexion.batch.Igniter"

    if (force) {
        def expressionRule = findExpressionRule(name: igniterName, new ExpressionRuleFinder(Context.getSDKContext()))
        if (expressionRule) { expressionRule.delete() }

        def ruleTimer = findRuleTimer(name: igniterName, new RuleTimerFinder(Context.getSDKContext()))
        if (ruleTimer) { ruleTimer.delete() }
    }

    createExpressionRule(
        name            : igniterName,
        description     : "Executes the Batch Igniter",
        ifExpression    : new Expression("true"),
        thenExpression  : new Expression("ExecuteCustomObject(\"BatchIgniter\")"),
        elseExpression  : new Expression(),
        enabled         : true
    )

    def expressionRule = findExpressionRule(name: igniterName, new ExpressionRuleFinder(Context.getSDKContext()))

    createRuleTimer(name: igniterName, description: "Executes the Batch Iginiter", cronSchedule: cronSchedule, rules: [expressionRule])
}

private void scheduleFinalizer(String cronSchedule, Boolean force) {
    String name = "connexion.batch.Finalizer"

    if (force) {
        def expressionRule = findExpressionRule(name: name, new ExpressionRuleFinder(Context.getSDKContext()))
        if (expressionRule) { expressionRule.delete() }

        def ruleTimer = findRuleTimer(name: name, new RuleTimerFinder(Context.getSDKContext()))
        if (ruleTimer) { ruleTimer.delete() }
    }

    createExpressionRule(
        name            : name,
        description     : "Executes the Batch Finalizer",
        ifExpression    : new Expression("true"),
        thenExpression  : new Expression("ExecuteCustomObject(\"BatchFinalizer\")"),
        elseExpression  : new Expression(),
        enabled         : true
    )

    def expressionRule = findExpressionRule(name: name, new ExpressionRuleFinder(Context.getSDKContext()))

    createRuleTimer(name: name, description: "Executes the Batch Finalizer", cronSchedule: cronSchedule, rules: [expressionRule])
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