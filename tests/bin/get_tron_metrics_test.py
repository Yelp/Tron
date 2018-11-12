import subprocess

import mock
import pytest

from tron.bin import get_tron_metrics


def test_send_data_metric():
    process = mock.Mock()
    process.communicate = mock.Mock(return_value=(b'fake_output', b'fake_error'))
    cmd_str = (
        'meteorite data -v fake_name fake_metric_type fake_value '
        '-d fake_dim_key:fake_dim_value'
    )

    with mock.patch(
        'subprocess.Popen',
        mock.Mock(return_value=process),
        autospec=None,
    ) as mock_popen:
        get_tron_metrics.send_data_metric(
            name='fake_name',
            metric_type='fake_metric_type',
            value='fake_value',
            dimensions={'fake_dim_key': 'fake_dim_value'},
            dry_run=False,
        )

        assert mock_popen.call_count == 1
        assert mock_popen.call_args == mock.call(
            cmd_str.split(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )


def test_send_data_metric_dry_run():
    with mock.patch('subprocess.Popen', autospec=True) as mock_popen:
        get_tron_metrics.send_data_metric(
            name='fake_name',
            metric_type='fake_metric_type',
            value='fake_value',
            dimensions='fake_dimensions',
            dry_run=True,
        )

        assert mock_popen.call_count == 0


@mock.patch('tron.bin.get_tron_metrics.send_data_metric', autospec=True)
def test_send_counter(mock_send_data_metric):
    kwargs = dict(count='fake_count')

    get_tron_metrics.send_counter('fake_name', **kwargs)

    assert mock_send_data_metric.call_count == 1
    assert mock_send_data_metric.call_args == mock.call(
        name='fake_name',
        metric_type='counter',
        value='fake_count',
        dimensions={},
        dry_run=False,
    )


@mock.patch('tron.bin.get_tron_metrics.send_data_metric', autospec=True)
def test_send_gauge(mock_send_data_metric):
    kwargs = dict(value='fake_value')

    get_tron_metrics.send_gauge('fake_name', **kwargs)

    assert mock_send_data_metric.call_count == 1
    assert mock_send_data_metric.call_args == mock.call(
        name='fake_name',
        metric_type='gauge',
        value='fake_value',
        dimensions={},
        dry_run=False,
    )


@mock.patch('tron.bin.get_tron_metrics.send_counter', autospec=True)
def test_send_meter(mock_send_counter):
    get_tron_metrics.send_meter('fake_name')

    assert mock_send_counter.call_count == 1
    assert mock_send_counter.call_args == mock.call('fake_name')


@mock.patch('tron.bin.get_tron_metrics.send_gauge', autospec=True)
def test_send_histogram(mock_send_gauge):
    kwargs = dict(
        p50='fake_p50',
        p75='fake_p75',
        p95='fake_p95',
        p99='fake_p99',
    )
    p50_kwargs = dict(
        **kwargs,
        value='fake_p50'
    )

    get_tron_metrics.send_histogram('fake_name', **kwargs)

    assert mock_send_gauge.call_count == len(kwargs)
    assert mock_send_gauge.call_args_list[0] == mock.call(
        'fake_name.p50',
        **p50_kwargs,
    )


@mock.patch('tron.bin.get_tron_metrics.send_meter', autospec=True)
@mock.patch('tron.bin.get_tron_metrics.send_histogram', autospec=True)
def test_send_timer(mock_send_meter, mock_send_histogram):
    get_tron_metrics.send_timer('fake_name')

    assert mock_send_meter.call_count == 1
    assert mock_send_meter.call_args == mock.call('fake_name')
    assert mock_send_histogram.call_count == 1
    assert mock_send_histogram.call_args == mock.call('fake_name')


@pytest.mark.parametrize('cluster', ['fake_cluster', None])
def test_send_metrics(cluster):
    mock_send_counter = mock.Mock()
    metrics = dict(counter=[dict(name='fake_name')])

    with mock.patch(
        'tron.bin.get_tron_metrics._METRIC_SENDERS',
        dict(counter=mock_send_counter),
        autospec=None,
    ):
        get_tron_metrics.send_metrics(metrics, cluster=cluster, dry_run=True)

    assert mock_send_counter.call_count == 1
    if cluster:
        assert mock_send_counter.call_args == mock.call(
            'fake_name',
            dry_run=True,
            dimensions={'tron_cluster': 'fake_cluster'},
        )
    else:
        assert mock_send_counter.call_args == mock.call('fake_name', dry_run=True)
