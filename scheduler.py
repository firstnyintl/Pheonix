import os
from apscheduler.schedulers.blocking import BlockingScheduler
from streetaccount import getMessages

if __name__ == '__main__':
    scheduler = BlockingScheduler()
    update_interval_min = 3
    scheduler.add_job(getMessages, 'interval', minutes=update_interval_min)
    print('Fetching new events every ' + update_interval_min + ' minutes')
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
