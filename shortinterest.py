import BBG
import datetime
import pdb


def main():
    index = 'SPX Index'

    stocks = BBG.getIndexMembers(index)

    data = BBG.getFields(stocks, ['EXPECTED_REPORT_DT', 'SHORT_INT_RATIO'])

    shortThreshold = 4

    today = datetime.datetime.now().date()

    data['earningsToday'] = data.EXPECTED_REPORT_DT == today

    data['highSIratio'] = data.SHORT_INT_RATIO > shortThreshold

    output = data[data.earningsToday & data.highSIratio]

    print output

if __name__ == '__main__':
    main()
