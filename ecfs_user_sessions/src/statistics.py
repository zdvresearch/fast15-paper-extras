import numpy
import scipy
import scipy.stats

def percentile(N, P):
    """
    Find the percentile of a list of values

    @parameter N - A list of values.  N must be sorted.
    @parameter P - A float value from 0.0 to 1.0

    @return - The percentile of the values.
    """
    n = int(round(P * len(N) + 0.5))
    return N[n-1]

def mean_confidence_interval(data, confidence=0.95):
    a = 1.0 * numpy.array(data)
    n = len(a)
    m, se = numpy.mean(a), scipy.stats.sem(a)
    # calls the inverse CDF of the Student's t distribution
    h = se * scipy.stats.t._ppf((1 + confidence) / 2., n - 1)
    return m - h, m + h

def mean_confidence_interval_h(data, confidence=0.95):
    a = 1.0 * numpy.array(data)
    n = len(a)
    m, se = numpy.mean(a), scipy.stats.sem(a)
    # calls the inverse CDF of the Student's t distribution
    h = se * scipy.stats.t._ppf((1 + confidence) / 2., n - 1)
    return h

def mean_confidence_interval_with_error(data, confidence=0.95):
    a = 1.0 * numpy.array(data)
    n = len(a)
    m, se = numpy.mean(a), scipy.stats.sem(a)
    # calls the inverse CDF of the Student's t distribution
    h = se * scipy.stats.t._ppf((1 + confidence) / 2., n - 1)
    return m, h

def mean(data, places=2):
    a = 1.0 * numpy.array(data)
    return round(numpy.mean(a), places)

def get_mean_string(data):
    m = mean(data)
    mm, mp = mean_confidence_interval(data)
    return "%.1f" % (round(m, 1))

def get_meanconf_string(data):
    m = mean(data)
    mm, mp = mean_confidence_interval(data)
    return "%s ($\pm$ %s)" % ('{:,.2f}'.format(m), '{:,.2f}'.format(m - mm, 1))
