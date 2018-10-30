import mock
import pytest

import tron.metrics as metrics


@pytest.fixture(autouse=True)
def all_metrics():
    with mock.patch.object(metrics, 'all_metrics', new=dict()) as mock_all:
        yield mock_all


def test_get_metric(all_metrics):
    timer = metrics.get_metric(
        'timer', 'api.requests', {'method': 'GET'}, mock.Mock()
    )
    same_timer = metrics.get_metric(
        'timer', 'api.requests', {'method': 'GET'}, mock.Mock()
    )
    other_timer = metrics.get_metric(
        'timer', 'api.requests', {'method': 'POST'}, mock.Mock()
    )
    metrics.get_metric('something', 'name', None, mock.Mock())
    assert timer == same_timer
    assert other_timer != timer
    assert len(all_metrics) == 3


@mock.patch('tron.metrics.get_metric', autospec=True)
def test_timer(mock_get_metric):
    test_metric = metrics.Timer()
    mock_get_metric.return_value = test_metric
    metrics.timer('my_metric', 110)
    metrics.timer('my_metric', 84)
    mock_get_metric.assert_called_with(
        'timer',
        'my_metric',
        None,
        mock.ANY,
    )
    result = metrics.view_timer(test_metric)
    assert result['count'] == 2


@mock.patch('tron.metrics.get_metric', autospec=True)
def test_count(mock_get_metric):
    test_metric = metrics.Counter()
    mock_get_metric.return_value = test_metric
    metrics.count('my_metric', 13)
    metrics.count('my_metric', -1)
    mock_get_metric.assert_called_with(
        'counter',
        'my_metric',
        None,
        mock.ANY,
    )
    result = metrics.view_counter(test_metric)
    assert result['count'] == 12


@mock.patch('tron.metrics.get_metric', autospec=True)
def test_meter(mock_get_metric):
    test_metric = metrics.Meter()
    mock_get_metric.return_value = test_metric
    metrics.meter('my_metric')
    metrics.meter('my_metric')
    mock_get_metric.assert_called_with(
        'meter',
        'my_metric',
        None,
        mock.ANY,
    )
    result = metrics.view_meter(test_metric)
    assert result['count'] == 2


@mock.patch('tron.metrics.get_metric', autospec=True)
def test_gauge(mock_get_metric):
    test_metric = metrics.SimpleGauge()
    mock_get_metric.return_value = test_metric
    metrics.gauge('my_metric', 23)
    metrics.gauge('my_metric', 102)
    mock_get_metric.assert_called_with(
        'gauge',
        'my_metric',
        None,
        mock.ANY,
    )
    result = metrics.view_gauge(test_metric)
    assert result['value'] == 102


@mock.patch('tron.metrics.get_metric', autospec=True)
def test_histogram(mock_get_metric):
    test_metric = metrics.Histogram()
    mock_get_metric.return_value = test_metric
    metrics.histogram('my_metric', 2)
    metrics.histogram('my_metric', 92)
    mock_get_metric.assert_called_with(
        'histogram',
        'my_metric',
        None,
        mock.ANY,
    )
    result = metrics.view_histogram(test_metric)
    assert result['count'] == 2


def test_view_all_metrics_empty():
    result = metrics.view_all_metrics()
    assert result == {
        'counter': [],
        'gauge': [],
        'histogram': [],
        'meter': [],
        'timer': [],
    }


def test_view_all_metrics():
    metrics.timer('a', 1)
    metrics.count('b', 9, dimensions={'method': 'GET'})
    metrics.meter('c')
    metrics.gauge('d', 3)
    metrics.histogram('e', 2)
    metrics.histogram('f', 3)
    result = metrics.view_all_metrics()

    assert len(result['timer']) == 1
    assert result['timer'][0]['name'] == 'a'

    assert len(result['counter']) == 1
    assert result['counter'][0]['name'] == 'b'
    assert result['counter'][0]['dimensions'] == {'method': 'GET'}

    assert len(result['meter']) == 1
    assert result['meter'][0]['name'] == 'c'

    assert len(result['gauge']) == 1
    assert result['gauge'][0]['name'] == 'd'

    assert len(result['histogram']) == 2
    names = set(metric['name'] for metric in result['histogram'])
    assert names == set(['e', 'f'])
