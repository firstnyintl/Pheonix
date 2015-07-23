import json

adr_local = json.dumps([
    {'type': 'title',
     'title': 'Basic Settings'},
    {'type': 'bool',
     'title': 'Exclude ORD holidays',
     'desc': 'Exclude signals on days before ORD holidays',
     'section': 'ADR',
     'key': 'exclude_ord_holidays'},
    {'type': 'bool',
     'title': 'Exclude Dividends',
     'desc': 'Exclude signals on/between dividend dates',
     'section': 'ADR',
     'key': 'exclude_dividends'},
    {'type': 'bool',
     'title': 'Exclude Earnings',
     'desc': 'Exclude signals on/the day before earnings',
     'section': 'ADR',
     'key': 'exclude_earnings'}])
