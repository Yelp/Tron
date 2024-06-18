import datetime
import json
import logging
import operator
import socket
from functools import lru_cache
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

import staticconf  # type: ignore

from tron.config.static_config import get_config_watcher
from tron.config.static_config import NAMESPACE


try:
    from scribereader import scribereader
    from scribereader.clog.readers import StreamTailerSetupError

    scribereader_available = True
except ImportError:
    scribereader_available = False  # sorry folks, you'll need to add your own way to retrieve logs

try:
    from clog.readers import S3LogsReader

    s3reader_available = True
except ImportError:
    s3reader_available = False


log = logging.getLogger(__name__)
USE_SRV_CONFIGS = -1


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
def get_scribereader_host_and_port(ecosystem: str, superregion: str, region: str) -> Tuple[str, int]:
    # NOTE: Passing in an ecosystem of prod is not supported by scribereader
    # as there's no mapping of ecosystem->scribe-kafka-services discovery hosts
    # for this ecosystem
    host, port = scribereader.get_tail_host_and_port(
        ecosystem=ecosystem if ecosystem != "prod" else None,
        region=region,
        superregion=superregion,
    )
    return host, port


class PaaSTALogs:
    def __init__(self, component: str, paasta_cluster: str, action_run_id: str) -> None:
        self.component = component
        self.paasta_cluster = paasta_cluster
        self.action_run_id = action_run_id
        namespace, job_name, run_num, action = action_run_id.split(".")
        # in our logging infra, things are logged to per-instance streams - but
        # since Tron PaaSTA instances are of the form `job_name.action`, we need
        # to escape the period since some parts of our infra will reject streams
        # containing them - thus, the "weird" __ separator
        self.stream_name = f"stream_paasta_app_output_{namespace}_{job_name}__{action}"
        self.run_num = int(run_num)
        self.num_lines = 0
        self.malformed_lines = 0
        self.output: List[Tuple[str, str]] = []
        self.truncated_output = False

    def fetch(self, stream: Iterator[str], max_lines: Optional[int]) -> None:
        for line in stream:
            if max_lines is not None and self.num_lines == max_lines:
                self.truncated_output = True
                break
            # it's possible for jobs to run multiple times a day and have obscenely large amounts of output
            # so we can't just truncate after seeing X number of lines for the run number in question - we
            # need to count how many total lines we've seen and bail out early to preserve tron's uptime
            self.num_lines += 1

            try:
                payload = json.loads(line)
            except json.decoder.JSONDecodeError:
                log.error(
                    f"Unable to decode log line from stream ({self.stream_name}) for {self.action_run_id}: {line}"
                )
                self.malformed_lines += 1
                continue

            if (
                int(payload.get("tron_run_number", -1)) == self.run_num
                and payload.get("component") == self.component
                and payload.get("message") is not None
                and payload.get("timestamp") is not None
                and payload.get("cluster") == self.paasta_cluster
            ):
                self.output.append((payload["timestamp"], payload["message"]))

    def sorted_lines(self) -> List[str]:
        self.output.sort(key=operator.itemgetter(0))
        return [line for _, line in self.output]


def read_log_stream_for_action_run(
    action_run_id: str,
    component: str,
    min_date: Optional[datetime.datetime],
    max_date: Optional[datetime.datetime],
    paasta_cluster: Optional[str],
    max_lines: Optional[int] = USE_SRV_CONFIGS,
) -> List[str]:
    if min_date is None:
        return [f"{action_run_id} has not started yet."]

    use_s3_reader = False

    if s3reader_available:
        config_watcher = get_config_watcher()
        config_watcher.reload_if_changed()
        use_s3_reader = staticconf.read("logging.use_s3_reader", namespace=NAMESPACE, default=False)
    elif scribereader_available is False:
        return ["Neither scribereader nor yelp_clog (internal Yelp packages) are available - unable to display logs."]

    if max_lines == USE_SRV_CONFIGS:
        config_watcher = get_config_watcher()
        config_watcher.reload_if_changed()
        max_lines = staticconf.read("logging.max_lines_to_display", namespace=NAMESPACE)

    try:
        ecosystem = get_ecosystem()
        superregion = get_superregion()
        region = get_region()
    except OSError:
        log.warning("Unable to read location mapping files from disk, not returning scribereader host/port")
        return [
            "Unable to determine where Tron is located. If you're seeing this inside Yelp, report this to #compute-infra"
        ]

    if paasta_cluster is None:
        paasta_cluster = superregion

    paasta_logs = PaaSTALogs(component, paasta_cluster, action_run_id)
    stream_name = paasta_logs.stream_name
    end_date: Optional[datetime.date]

    # yelp_clog S3LogsReader is a newer reader that is supposed to replace scribe readers eventually.
    if use_s3_reader:
        # S3 reader accepts datetime objects and respects timezone information
        # if min_date and max_date timezone is missing, astimezone() will assume local timezone and convert it to UTC
        start_datetime = min_date.astimezone(datetime.timezone.utc)
        end_datetime = (
            max_date.astimezone(datetime.timezone.utc)
            if max_date
            else datetime.datetime.now().astimezone(datetime.timezone.utc)
        )

        log.debug("Using S3LogsReader to retrieve logs")
        s3_reader = S3LogsReader(superregion).get_log_reader(
            log_name=stream_name, start_datetime=start_datetime, end_datetime=end_datetime
        )
        paasta_logs.fetch(s3_reader, max_lines)
    else:
        today = datetime.date.today()
        start_date = min_date.date()
        end_date = max_date.date() if max_date else None
        use_tailer = today in {start_date, end_date}
        use_reader = start_date != today and end_date is not None

        host, port = get_scribereader_host_and_port(ecosystem, superregion, region)

        # We'll only use a stream reader for logs from not-today.
        # that said, it's possible that an action spans more than a single day - in this case, we'll first read "historical" data from
        # the reader and then follow-up with today's logs from a stream tailer.
        # NOTE: this is more-or-less what our internal `scribereader` binary does
        if use_reader:
            with scribereader.get_stream_reader(
                stream_name=stream_name,
                min_date=min_date,
                max_date=max_date,
                reader_host=host,
                reader_port=port,
            ) as stream:
                paasta_logs.fetch(stream, max_lines)

        if use_tailer:
            stream = scribereader.get_stream_tailer(
                stream_name=stream_name,
                tailing_host=host,
                tailing_port=port,
                lines=-1,
            )
            try:
                paasta_logs.fetch(stream, max_lines)
            except StreamTailerSetupError:
                return [
                    f"No data in stream {stream_name} - if this is the first time this action has run and you expected "
                    "output, please wait a couple minutes and refresh."
                ]
            except socket.timeout:
                return [
                    f"Unable to connect to stream {stream_name} - if this is the first time this action has run and you "
                    "expected output, please wait a couple minutes and refresh."
                ]
            finally:
                stream.close()

    # for logs that use Kafka topics with multiple partitions underneath or retrieved by S3LogsReader,
    # data ordering is not guaranteed - so we'll sort based on log timestamp set by producer.
    lines = paasta_logs.sorted_lines()
    malformed = (
        [f"{paasta_logs.malformed_lines} encountered while retrieving logs"] if paasta_logs.malformed_lines else []
    )
    try:
        location_selector = f"-s {paasta_cluster}" if "prod" in paasta_cluster else f'-e {paasta_cluster.split("-")[1]}'
    except IndexError:
        location_selector = f"-s {paasta_cluster}"
    truncation_message = (
        [
            f"This output is truncated. Use this command to view all lines: scribereader {location_selector} {stream_name} --min-date {min_date.date()} --max-date {max_date.date()} | jq --raw-output 'select(.tron_run_number=={int(paasta_logs.run_num)} and .component == \"{component}\") | .message'"
        ]
        if max_date
        else [
            f"This output is truncated. Use this command to view all lines: scribereader {location_selector} {stream_name} --min-date {min_date.date()} | jq --raw-output 'select(.tron_run_number=={int(paasta_logs.run_num)} and .component == \"{component}\") | .message'"
        ]
    )
    truncated = truncation_message if paasta_logs.truncated_output else []

    return lines + malformed + truncated
