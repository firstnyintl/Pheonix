import blpapi
from optparse import OptionParser
import pandas as pd
import numpy as np
import pytz
import pdb
import datetime


def parseCmdLine():
    parser = OptionParser(description="Retrieve realtime data.")
    parser.add_option("-a",
                      "--ip",
                      dest="host",
                      help="server name or IP (default: %default)",
                      metavar="ipAddress",
                      default="localhost")
    parser.add_option("-p",
                      dest="port",
                      type="int",
                      help="server port (default: %default)",
                      metavar="tcpPort",
                      default=8194)
    # parser.add_option("--me",
    #                   dest="maxEvents",
    #                   type="int",
    #                   help="stop after this many events (default: %default)",
    #                   metavar="maxEvents",
    #                   default=1000000)

    (options, args) = parser.parse_args()

    return options


def createSession():
    """
    Creates Session
    --------------------------
    ARGS:
    RETURNS: Session object
    """
    options = parseCmdLine()
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(options.host)
    sessionOptions.setServerPort(options.port)
    session = blpapi.Session(sessionOptions)
    if not session.start():
        print "Failed to start session."
        return
    return session


def getSingleField(security, field):
    """
    Returns DataFrame with securities and fields
    """
    session = createSession()
    if not session.openService("//blp/refdata"):
        print "Failed to open //blp/refdata"
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("ReferenceDataRequest")

    request.append('securities', security)
    request.append("fields", field)

    session.sendRequest(request)

    loop = True
    try:
        while(loop):
            event = session.nextEvent()
            for msg in event:
                if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    securityDataArray = msg.getElement(blpapi.Name("securityData"))
                    for securityData in securityDataArray.values():
                        security = securityData.getElementAsString(blpapi.Name("security"))
                        fieldData = securityData.getElement(blpapi.Name("fieldData"))
                        if fieldData.numElements() == 0: raise ValueError('N/A Value')
                        for field in fieldData.elements():
                            return field.getValue()
    finally:
        endSession(session)


def updateHistoricalTickData(security, max_days=120):
    """
    Update historical tick data through today
    """
    session = createSession()
    if not session.openService("//blp/refdata"):
        print "Failed to open //blp/refdata"
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("IntradayTickRequest")

    # Dataset filepath
    DBfile = 'TickData/' + security.replace(' ', '_') + '.h5'

    # Open DataBase store
    store = pd.HDFStore(DBfile)

    # Path to dataset within file
    dataset_path = 'ticks'

    # Set security for request
    request.set('security', security)

    # Set type of event for request (TRADE)
    request.append("eventTypes", 'TRADE')
    request.append("eventTypes", 'AT_TRADE')

    # Get todays datetime at 00:00
    today = datetime.datetime.today()
    today = datetime.datetime(day=today.day, month=today.month, year=today.year)
    # Set timezone as New York
    today = pytz.timezone('America/New_York').localize(today)

    # Convert to UTC
    today = today.astimezone(pytz.timezone('UTC'))

    # Set end to today at 00:00
    request.set("endDateTime", today)

    # Get all data (SNEAKY FUCKING BLOOMBERG)
    request.set("includeNonPlottableEvents", "True")
    request.set("includeConditionCodes", "True")

    # Check if dataframe already exists, get last index
    try:
        # Find last entry in stored tickdata
        last_index = store[dataset_path].index.max()

        # Set start to next day at 00:00
        next_day = last_index + datetime.timedelta(days=1)
        start = datetime.datetime(day=next_day.day, month=next_day.month, year=next_day.year)

        # Set as New York
        start = pytz.timezone('America/New_York').localize(start)

        # Convert to UTC
        start = start.astimezone(pytz.timezone('UTC'))

        # If no new days passed since last update, exit
        if today == start: return

        # Set end to yesterday
        # Set start time to next day after
        request.set("startDateTime", last_index + datetime.timedelta(days=1))

    except:
        # Set start to max_days before end
        start = today - datetime.timedelta(days=max_days)

        # Set start and end for request
        request.set("startDateTime", start)

    # Send request
    session.sendRequest(request)

    print '---- Started Processing ' + security + '----'

    loop = True
    # Start message loop
    try:
        while(loop):
            event = session.nextEvent()
            for msg in event:
                if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    tickDataArray = msg.getElement(blpapi.Name("tickData"))
                    tickDataArray = tickDataArray.getElement(1)

                    # Create dataframe for message
                    output = pd.DataFrame(columns=['Price', 'Size'])

                    # Loop through all trades
                    for eventData in tickDataArray.values():

                        # Get time
                        time = eventData.getElement(0).getValue(0)

                        # Datetime returned is GMT, make timezone aware
                        time = time.replace(tzinfo=pytz.timezone('UTC'))

                        # Convert Datetime to New York timezone
                        time = time.astimezone(pytz.timezone('America/New_York'))

                        # Get price of trade
                        price = eventData.getElement(2).getValue(0)

                        # Get size of trade
                        size = eventData.getElement(3).getValue(0)

                        # Create DataFrame
                        df = pd.DataFrame([[price, size]], columns=output.columns, index=[time])

                        # Append row to message dataframe
                        output = output.append(df)

                    # Append message data to database
                    store.append(dataset_path, output, min_itemsize=200, format='table', data_columns=True)
                    print security + ' write, last ' + str(time) 

                    # If final event, end loop
                    if event.eventType() == blpapi.Event.RESPONSE:
                        print '---- Finished Processing ' + security + '----'
                        loop = False

    finally:
        endSession(session)

    # Close DataBase access
    store.close()


def getHistoricalFields(securities, fields, startDate, endDate, periodicity='DAILY'):
    """
    Returns DataFrame with securities and fields
    """

    session = createSession()
    if not session.openService("//blp/refdata"):
        print "Failed to open //blp/refdata"
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("HistoricalDataRequest")

    if type(securities) == str:
        securities = [securities]
    if type(fields) == str:
        fields = [fields]

    if type(securities) is not pd.core.series.Series:
        securities = pd.Series(securities)
    if type(fields) is not pd.core.series.Series:
        fields = pd.Series(fields)

    for security in securities:
        request.append("securities", security)
    for field in fields:
        request.append("fields", field)
    request.set("startDate", startDate)
    request.set("endDate", endDate)
    request.set("periodicitySelection", periodicity)

    session.sendRequest(request)

    output = pd.DataFrame(index=securities.values, columns=fields)
    filled = []
    loop = True
    try:
        while(loop):
            event = session.nextEvent()
            for msg in event:
                if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    securityData = msg.getElement(blpapi.Name("securityData"))
                    security = securityData.getElementAsString(blpapi.Name("security"))
                    fieldData = securityData.getElement(blpapi.Name("fieldData"))
                    for field in range(fieldData.numValues()):
                        fields = fieldData.getValue(field)
                        for n in range(1, fields.numElements()):
                            name = str(fields.getElement(n).name())
                            output.loc[security][name] = fields.getElement(n).getValue()
                    filled.append(security)
            loop = not(len(filled) == len(output))
    finally:
        endSession(session)
    return output


def getFields(securities, fields):
    """
    Returns DataFrame with securities and fields
    """

    session = createSession()
    if not session.openService("//blp/refdata"):
        print "Failed to open //blp/refdata"
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("ReferenceDataRequest")

    if type(fields) == str:
        fields = [fields]
    if type(securities) == str:
        securities = [securities]

    if type(securities) is not pd.core.series.Series:
        securities = pd.Series(securities)
    if type(fields) is not pd.core.series.Series:
        fields = pd.Series(fields)

    for security in securities:
        request.append("securities", security)
    for field in fields:
        request.append("fields", field)

    session.sendRequest(request)

    output = pd.DataFrame(index=securities.values, columns=fields)
    filled = []
    loop = True
    try:
        while(loop):
            event = session.nextEvent()
            for msg in event:
                if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    securityDataArray = msg.getElement(blpapi.Name("securityData"))
                    for securityData in securityDataArray.values():
                        security = securityData.getElementAsString(blpapi.Name("security"))
                        fieldData = securityData.getElement(blpapi.Name("fieldData"))
                        for field in fieldData.elements():
                            output.loc[security][str(field.name())] = field.getValue(0)
                        filled.append(security)
            loop = not(len(filled) == len(output))
    finally:
        endSession(session)
    return output


def getFieldsOverride(securities, field, override_fields, override_values):
    """
    Returns DataFrame with securities and fields
    """

    session = createSession()
    if not session.openService("//blp/refdata"):
        print "Failed to open //blp/refdata"
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("ReferenceDataRequest")

    if type(securities) == str:
        securities = [securities]

    if type(securities) is not pd.core.series.Series:
        securities = pd.Series(securities)

    for security in securities:
        request.append("securities", security)
    request.append("fields", field)

    overrides = request.getElement("overrides")

    if type(override_fields) == str:
        override_fields = [override_fields]

    if type(override_values) == str:
        override_values = [override_values]

    for fld, val in zip(override_fields, override_values):
        override = overrides.appendElement()
        override.setElement("fieldId", fld)
        override.setElement("value", val)

    session.sendRequest(request)
    output = pd.DataFrame(index=securities.values, columns=[field])
    filled = []
    loop = True
    try:
        while(loop):
            event = session.nextEvent()
            for msg in event:
                if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    securityDataArray = msg.getElement(blpapi.Name("securityData"))
                    for securityData in securityDataArray.values():
                        security = securityData.getElementAsString(blpapi.Name("security"))
                        fieldData = securityData.getElement(blpapi.Name("fieldData"))
                        for field in fieldData.elements():
                            try:
                                fieldName = str(field.getValue(0).name())
                            except:
                                fieldName = ""
                            if fieldName == "ADVERTISED_TOTAL_VOLUME_RANK":
                                output.loc[security][fieldName] = field.getValue(0).getElement(2).getValue()
                            else:
                                output.loc[security][str(field.name())] = field.getValue(0)
                        filled.append(security)
            loop = not(len(filled) == len(output))
    finally:
        endSession(session)
    return output


def getBulkAnalystRecs(stock):
    """
    Returns DataFrame of historical analyst ratings for Equity security
    """
    session = createSession()
    if not session.openService("//blp/refdata"):
        print "Failed to open //blp/refdata"
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("ReferenceDataRequest")

    request.append('securities', stock + ' Equity')
    request.append('fields', 'BEST_ANALYST_RECS_BULK')

    session.sendRequest(request)
    loop = True
    try:
        while(loop):
            event = session.nextEvent()
            for msg in event:
                if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    securityDataArray = msg.getElement(blpapi.Name("securityData"))
                    for securityData in securityDataArray.values():
                        fieldData = securityData.getElement(blpapi.Name("fieldData"))
                        for field in fieldData.elements():
                            ratings = field.values()
                            output = pd.DataFrame(index=np.arange(field.numValues()), columns=['Firm Name', 'Analyst', 'Recommendation', 'Rating', 'Action Code', 'Target Price', 'Period', 'Date', 'BARR', '1 Year Return'])
                            for i, rating in enumerate(ratings):
                                for element in rating.elements():
                                    fld = element.name().__str__()
                                    output.loc[i][fld] = element.getValue()
                        return output
    finally:
        endSession(session)


def getIndexMembers(index):
    """
    Returns Equity index members as Pandas Series, opens and closes session
    """
    session = createSession()
    if not session.openService("//blp/refdata"):
        print "Failed to open //blp/refdata"
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("ReferenceDataRequest")

    request.append("securities", index)
    request.append("fields", "INDX_MEMBERS")

    session.sendRequest(request)

    output = []
    loop = True
    try:
        while(loop):
            event = session.nextEvent()
            for msg in event:
                if event.eventType() == blpapi.Event.RESPONSE:
                    securityDataArray = msg.getElement(blpapi.Name("securityData"))
                    for securityData in securityDataArray.values():
                        fieldData = securityData.getElement(blpapi.Name("fieldData"))
                        for field in fieldData.elements():
                            for val in field.values():
                                for symb in val.elements():
                                    output.append(symb.getValue() + " Equity")
                    loop = False
    finally:
        endSession(session)

    return pd.Series(output, name=index)


def endSession(session):
    """
    Ends Current Session
    --------------------------
    ARGS:
    session - Session object
    RETURNS:
    """
    session.stop()


def startSubscriptions(session, security, field, interval=0):
    """
    Start a subscription
    --------------------------
    ARGS:
    session - Session object
    security - Security
    field - Field
    """
    subscriptions = blpapi.SubscriptionList()
    subscriptions.add(security, field, "interval=%s"%(interval), blpapi.CorrelationId(security))
    session.subscribe(subscriptions)


def formatDate(date):
    """
    Takes datetime.date or datetime.datetime object and formats BBG date string
    """
    return date.strftime('%Y%m%d')


def RTmessageToPandas(message):
    """
    Convert a subscription blpapi Message Object to Pandas Series
    --------------------------
    ARGS:
    message - blpapi Message Object
    RETURNS:
    Pandas DataFrame object
    """

    output = {}

    # Get number of elements in Message Object
    num_elements = message.numElements()
    # Iterate through Elements
    for i in range(num_elements):
        # Get Element Name and convert to string
        name = str(message.getElement(i).name())
        # If Element is in Null State (no Value attached) post "N/A"
        if message.getElement(i).isNull() is True:
            value = "N/A"
        else:
            # If message is Name object convert to string
            try:
                if type(message.getElement(i).getValue()) == blpapi.name.Name:
                    value = str(message.getElement(i).getValue())
                else:
                    value = message.getElement(i).getValue()
            except ValueError:
                print "ValueError"
        output[name] = value

    return pd.Series(output)


def getNonSettlementDates(CDRCode):
    """
    Returns Series of dates given 'CDR_COUNTRY_CODE'
    """
    session = createSession()
    if not session.openService("//blp/refdata"):
        print "Failed to open //blp/refdata"
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("ReferenceDataRequest")

    request.append('securities', 'USD Curncy')
    request.append('fields', 'CALENDAR_NON_SETTLEMENT_DATES')

    overrides = request.getElement("overrides")
    override_fields = ['SETTLEMENT_CALENDAR_CODE', 'CALENDAR_START_DATE', 'CALENDAR_END_DATE']
    override_values = [CDRCode, '04/29/2015', '04/29/2015']

    for fld, val in zip(override_fields, override_values):
        override = overrides.appendElement()
        override.setElement("fieldId", fld)
        override.setElement("value", val)

    session.sendRequest(request)
    output = []
    loop = True
    try:
        while(loop):
            event = session.nextEvent()
            for msg in event:
                if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    securityDataArray = msg.getElement(blpapi.Name("securityData"))
                    for securityData in securityDataArray.values():
                        fieldData = securityData.getElement(blpapi.Name("fieldData"))
                        for field in fieldData.elements():
                            ratings = field.values()
                            for i, rating in enumerate(ratings):
                                for element in rating.elements():
                                    output.append(element.getValue())
                        return pd.Series(output)
    finally:
        endSession(session)


def isExchangeOpenByTickers(tickers, date):
    """
    Takes list-like tickers and returns DataFrame with
    boolean column whether was tradeable on that date
    """

    # Get CDR Country code for all tickers
    data = getFields(tickers, "CDR_COUNTRY_CODE")
    data['isOpen'] = True

    # Get unique country codes
    codes = data["CDR_COUNTRY_CODE"].unique().tolist()

    # Get isOpen values for all codes
    for code in codes:
        dates = getNonSettlementDates(code)
        if date in dates.tolist():
            data['isOpen'][data.CDR_COUNTRY_CODE == code] = False

    return data[['isOpen']]
