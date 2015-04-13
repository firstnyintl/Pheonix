from BBG import getSingleField


def test_getSingleField_Equity():
    security = 'MSFT'
    field = 'PX_LAST'
    assert type(getSingleField(security, field)) == float


def test_getSingleField_Currency():
    security = 'JPY'
    field = 'PX_LAST'
    stype = 'Curncy'
    assert type(getSingleField(security, field, securityType=stype)) == float


def test_getSingleField_Index():
    security = 'SPX'
    field = 'PX_LAST'
    stype = 'Index'
    assert type(getSingleField(security, field, securityType=stype)) == float


def test_getSingleField_Commodity():
    security = 'CLA'
    field = 'PX_LAST'
    stype = 'Comdty'
    assert type(getSingleField(security, field, securityType=stype)) == float


from BBG import getFields


def test_getFields_singleEquity_singleField():
    securities = 'MSFT'
    fields = 'PX_LAST'
    output = getFields(securities, fields)
    assert type(output['PX_LAST']['MSFT']) == float


def test_getFields_Equities_singleField():
    securities = ['MSFT', 'INTC']
    fields = 'PX_LAST'
    output = getFields(securities, fields)
    assert type(output['PX_LAST']['MSFT']) == float and type(output['PX_LAST']['INTC']) == float


def test_getFields_Equities_Fields():
    securities = ['MSFT', 'INTC']
    fields = ['PX_LAST', 'VOLUME']
    output = getFields(securities, fields)
    assert type(output['PX_LAST']['MSFT']) == float and type(output['PX_LAST']['INTC']) == float and type(output['VOLUME']['MSFT']) == float and type(output['VOLUME']['INTC']) == float


from BBG import getFieldsOverride


def test_getFieldsOverride_VWAP():
    securities = ['MSFT', 'INTC']
    field = 'VWAP'
    override_fields = ['VWAP_START_TIME', 'VWAP_END_TIME', 'VWAP_DT']
    override_values = ['00:00:00', '00:00:00', '20150410']
    output = getFieldsOverride(securities, field, override_fields, override_values)
    assert type(output['VWAP']['MSFT'] == float) and type(output['VWAP']['INTC'] == float)


from BBG import formatDate
from datetime import datetime


def test_formatDate():
    date = datetime(day=4, month=10, year=2014)
    assert formatDate(date) == '20141004'
