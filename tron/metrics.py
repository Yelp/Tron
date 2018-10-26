from pyformance.meters import Counter
from pyformance.meters import Histogram
from pyformance.meters import Meter
from pyformance.meters import SimpleGauge
from pyformance.meters import Timer

all_metrics = {}


def get_metric(metric_type, name, dimensions, default):
    global all_metrics
    dimensions = tuple(sorted(dimensions.items())) if dimensions else ()
    key = (metric_type, name, dimensions)
    return all_metrics.setdefault(key, default)


def timer(name, delta, dimensions=None):
    timer = get_metric('timer', name, dimensions, Timer())
    timer._update(delta)


def count(name, inc=1, dimensions=None):
    counter = get_metric('counter', name, dimensions, Counter())
    counter.inc(inc)


def meter(name, dimensions=None):
    meter = get_metric('meter', name, dimensions, Meter())
    meter.mark()


def gauge(name, value, dimensions=None):
    gauge = get_metric('gauge', name, dimensions, SimpleGauge())
    gauge.set_value(value)


def histogram(name, value, dimensions=None):
    histogram = get_metric('histogram', name, dimensions, Histogram())
    histogram.add(value)


def view_timer(timer):
    data = view_meter(timer)
    data.update(view_histogram(timer))
    return data


def view_counter(counter):
    return {'count': counter.get_count()}


def view_meter(meter):
    return {
        'count': meter.get_count(),
        'm1_rate': meter.get_one_minute_rate(),
        'm5_rate': meter.get_five_minute_rate(),
        'm15_rate': meter.get_fifteen_minute_rate(),
    }


def view_gauge(gauge):
    return {'value': gauge.get_value()}


def view_histogram(histogram):
    snapshot = histogram.get_snapshot()
    return {
        'count': histogram.get_count(),
        'mean': histogram.get_mean(),
        'min': histogram.get_min(),
        'max': histogram.get_max(),
        'p50': snapshot.get_median(),
        'p75': snapshot.get_75th_percentile(),
        'p95': snapshot.get_95th_percentile(),
        'p99': snapshot.get_99th_percentile(),
    }


metrics_to_viewers = {
    'counter': view_counter,
    'gauge': view_gauge,
    'histogram': view_histogram,
    'meter': view_meter,
    'timer': view_timer,
}


def view_all_metrics():
    all_data = {metric_type: [] for metric_type in metrics_to_viewers}
    for (metric_type, name, dims), metric in all_metrics.items():
        data = {'name': name, **metrics_to_viewers[metric_type](metric)}
        if dims:
            data.update({'dimensions': dict(dims)})
        all_data[metric_type].append(data)
    return all_data
