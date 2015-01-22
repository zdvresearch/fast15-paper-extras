__author__ = 'Matthias Grawinkel'

import calendar
import time

from datetime import datetime
from dateutil.rrule import rrule, DAILY, MONTHLY


class Timer():
    def __init__(self, s):
        self.s = s

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        print ("%s: %fs" % (self.s, (time.time() - self.start)))


def unix_time(dt):
    epoch = datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds()


def epoch_to_timestamp(epoch):
    return time.strftime("%Y %m/%d %H:%M:%S", time.gmtime(epoch))


def get_epochs(from_epoch, to_epoch, timespan):
    """

    @param from_epoch:
    @param to_epoch:
    @param timespan: "daily", "monthly", "yearly"
    @return:
    """
    from_time = time.gmtime(from_epoch)
    to_time = time.gmtime(to_epoch)

    date_from = datetime(from_time.tm_year, from_time.tm_mon, from_time.tm_mday, 0, 0, 0, 0)
    date_to = datetime(to_time.tm_year, to_time.tm_mon, to_time.tm_mday, 23, 59, 59, 999)

    epochs = []

    if timespan == "daily":
        for dt in rrule(DAILY, dtstart=date_from, until=date_to):

            e0 = unix_time(datetime(dt.year, dt.month, dt.day , 0, 0, 0, 0))
            e1 = unix_time(datetime(dt.year, dt.month, dt.day,  23, 59, 59, 999))
            epochs.append((e0, e1))

    elif timespan == "monthly":
        for dt in rrule(MONTHLY, dtstart=date_from, until=date_to):
            e0 = unix_time(datetime(dt.year, dt.month, 1, 0, 0, 0, 0))

            days_in_month = calendar.monthrange(dt.year, dt.month)[1]
            e1 = unix_time(datetime(dt.year, dt.month, days_in_month,  23, 59, 59, 999))
            epochs.append((e0, e1))

    elif timespan == "yearly":
        for year in range(from_time.tm_year, to_time.tm_year +1):
            e0 = unix_time(datetime(year, 1, 1, 0, 0, 0, 0))
            e1 = unix_time(datetime(year, 12, 31,  23, 59, 59, 999))
            epochs.append((e0, e1))

    return epochs


#
#e1 = 1231671119
##e2 = 1231671130
#e2 = 1271871312
#
#print "-=------------"
#print epoch_to_timestamp(e1)
#print epoch_to_timestamp(e2)
#print "-=------------"
#
#for t in get_epochs(e1, e2, "daily"):
#    print t, epoch_to_timestamp(t[0]), epoch_to_timestamp(t[1])
