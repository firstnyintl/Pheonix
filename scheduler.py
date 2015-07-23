import os
from apscheduler.schedulers.blocking import BlockingScheduler
from streetaccount import getMessages
from Data import updateTickData

if __name__ == '__main__':

    # Create scheduler
    sched = BlockingScheduler()

    # Schedule streetaccount messages
    interval = 1
    sched.add_job(getMessages, 'interval',  minutes=interval)
    print('Fetching new StreetAccount messages every ' + str(interval) + ' minutes')

    # Schedule tick data updates business days at 5am
    sched.add_job(updateTickData, 'cron', day_of_week='mon-fri', hour=5)
    print('Updating tick data business days at 5am')

    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        sched.start()

    except (KeyboardInterrupt, SystemExit):
        pass
