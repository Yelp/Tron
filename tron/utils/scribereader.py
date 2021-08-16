import datetime
import json
import logging
import operator
import socket
from functools import lru_cache
from typing import List
from typing import Optional
from typing import Tuple


try:
    from scribereader import scribereader  # type: ignore
    from clog.readers import StreamTailerSetupError  # type: ignore
except ImportError:
    scribereader = None  # sorry folks, you'll need to add your own way to retrieve logs


log = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_region() -> str:
    """
    Discover what region we're running in by reading this information from on-disk facts.

    Yelpers: for more information, see y/habitat
    """
    with open("/nail/etc/region") as f:
        return f.read().strip()


@lru_cache(maxsize=1)
def get_superregion() -> str:
    """
    Discover what region we're running in by reading this information from on-disk facts.

    Yelpers: for more information, see y/habitat
    """
    with open("/nail/etc/superregion") as f:
        return f.read().strip()


@lru_cache(maxsize=1)
def get_ecosystem() -> str:
    """
    Discover what ecosystem we're running in by reading this information from on-disk facts.

    Yelpers: for more information, see y/habitat
    """
    with open("/nail/etc/ecosystem") as f:
        return f.read().strip()


@lru_cache(maxsize=1)
def get_scribereader_host_and_port() -> Optional[Tuple[str, int]]:
    try:
        ecosystem = get_ecosystem()
        superregion = get_superregion()
        region = get_region()
    except OSError:
        log.warning("Unable to read location mapping files from disk, not returning scribereader host/port")
        return None

    host, port = scribereader.get_tail_host_and_port(
        ecosystem=ecosystem if ecosystem != "prod" else None, region=region, superregion=superregion,
    )
    return host, port


def read_log_stream_for_action_run(
    action_run_id: str, component: str, min_date: Optional[datetime.datetime], max_date: Optional[datetime.datetime],
) -> List[str]:
    if min_date is None:
        return [f"{action_run_id} has not started yet."]

    if scribereader is None:
        return ["Scribereader (an internal Yelp package) is not available - unable to display logs."]
    if get_scribereader_host_and_port() is None:
        return [
            "Unable to determine where Tron is located. If you're seeing this inside Yelp, report this to #compute-infra"
        ]

    host, port = get_scribereader_host_and_port()  # type: ignore  # the None case is covered by the check above

    today = datetime.date.today()
    start_date = min_date.date()
    end_date = max_date.date() if max_date else None

    use_tailer = today in {start_date, end_date}
    use_reader = start_date != today and end_date is not None

    if end_date is not None and end_date == today:
        end_date -= datetime.timedelta(days=1)

    namespace, job_name, run_num, action = action_run_id.split(".")
    # in our logging infra, things are logged to per-instance streams - but
    # since Tron PaaSTA instances are of the form `job_name.action`, we need
    # to escape the period since some parts of our infra will reject streams
    # containing them - thus, the "weird" __ separator
    stream_name = f"stream_paasta_app_output_{namespace}_{job_name}__{action}"
    output: List[Tuple[str, str]] = []

    # We'll only use a stream reader for logs from not-today.
    # that said, it's possible that an action spans more than a single day - in this case, we'll first read "historical" data from
    # the reader and then follow-up with today's logs from a stream tailer.
    # NOTE: this is more-or-less what our internal `scribereader` binary does
    if use_reader:
        with scribereader.get_stream_reader(
            stream_name=stream_name, min_date=min_date, max_date=max_date, reader_host=host, reader_port=port,
        ) as stream:
            for line in stream:
                try:
                    payload = json.loads(line)
                except json.decoder.JSONDecodeError:
                    log.error(f"Unable to decode log line from stream ({stream_name}) for {action_run_id}: {line}")
                    continue

                if (
                    payload.get("tron_run_number") == int(run_num)
                    and payload.get("component") == component
                    and payload.get("message") is not None
                    and payload.get("timestamp") is not None
                ):
                    output.append((payload["timestamp"], payload["message"]))

    if use_tailer:
        stream = scribereader.get_stream_tailer(
            stream_name=stream_name, tailing_host=host, tailing_port=port, lines=-1,
        )
        try:
            for line in stream:
                try:
                    payload = json.loads(line)
                except json.decoder.JSONDecodeError:
                    log.error(f"Unable to decode log line from stream ({stream_name}) for {action_run_id}: {line}")
                    continue

                if (
                    payload.get("tron_run_number") == int(run_num)
                    and payload.get("component") == component
                    and payload.get("message") is not None
                ):
                    output.append((payload["timestamp"], payload["message"]))
        except StreamTailerSetupError:
            return [
                f"No data in stream {stream_name} - if this is the first time this action has run, please wait a minute and refresh."
            ]
        except socket.timeout:
            return [
                f"Unable to connect to stream {stream_name} - if this is the first time this action has run, please wait a couple minutes and refresh."
            ]
        finally:
            stream.close()

    # XXX: for some reason, we're occasionally getting data out of order from scribereader - so we'll sort based on
    # timestamp until we can figure out what's causing this.
    output.sort(key=operator.itemgetter(0))
    return [line for _, line in output]
