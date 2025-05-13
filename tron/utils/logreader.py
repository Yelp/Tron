import datetime
import json
import logging
import operator
from functools import lru_cache
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

import staticconf
import yaml

from tron.config.static_config import get_config_watcher
from tron.config.static_config import NAMESPACE

try:
    from logreader.readers import S3LogsReader  # type: ignore[import-not-found]  # this is a private dependency

    s3reader_available = True
except ImportError:
    s3reader_available = False

    class S3LogsReader:  # type: ignore[no-redef]  # this is a private dependency
        def __init__(self, superregion: str) -> None:
            raise ImportError("logreader (internal Yelp package) is not available - unable to display logs.")

        def get_log_reader(
            self, log_name: str, start_datetime: datetime.datetime, end_datetime: datetime.datetime
        ) -> Iterator[str]:
            raise NotImplementedError("logreader (internal Yelp package) is not available - unable to display logs.")


log = logging.getLogger(__name__)
USE_SRV_CONFIGS = -1


@lru_cache(maxsize=1)
def get_superregion() -> str:
    """
    Discover what region we're running in by reading this information from on-disk facts.

    Yelpers: for more information, see y/habitat
    """
    with open("/nail/etc/superregion") as f:
        return f.read().strip()


def decompose_action_id(action_run_id: str, paasta_cluster: str) -> Tuple[str, str, str, str]:
    namespace, job_name, run_num, action = action_run_id.split(".")
    for ext in ["yaml", "yml"]:
        try:
            with open(f"/nail/etc/services/{namespace}/tron-{paasta_cluster}.{ext}") as f:
                config = yaml.load(f, Loader=yaml.CSafeLoader)
                service: Optional[str] = (
                    config.get(job_name, {}).get("actions", {}).get(action, {}).get("service", None)
                )
                if service:
                    return service, job_name, run_num, action
        except FileNotFoundError:
            log.warning(f"yelp-soaconfig file tron-{paasta_cluster}.{ext} not found for action_run_id {action_run_id}.")
        except yaml.YAMLError:
            log.exception(
                f"Error parsing YAML file tron-{paasta_cluster}.yaml for {action_run_id} - will default to using current namespace:"
            )
        except Exception:
            log.exception(
                f"Error reading service for {action_run_id} from file tron-{paasta_cluster}.yaml - will default to using current namespace:"
            )

    return namespace, job_name, run_num, action


class PaaSTALogs:
    def __init__(self, component: str, paasta_cluster: str, action_run_id: str) -> None:
        self.component = component
        self.paasta_cluster = paasta_cluster
        self.action_run_id = action_run_id
        namespace, job_name, run_num, action = decompose_action_id(action_run_id, paasta_cluster)
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

    if not s3reader_available:
        return ["logreader (internal Yelp package) is not available - unable to display logs."]

    if max_lines == USE_SRV_CONFIGS:
        config_watcher = get_config_watcher()
        config_watcher.reload_if_changed()
        max_lines = staticconf.read("logging.max_lines_to_display", namespace=NAMESPACE)  # type: ignore[attr-defined]  # staticconf has a lot of magic that mypy does not like

    try:
        superregion = get_superregion()
    except OSError:
        log.warning("Unable to read location mapping files from disk (/nail/etc/)")
        return [
            "Unable to determine where Tron is located. If you're seeing this inside Yelp, report this to #compute-infra"
        ]

    if paasta_cluster is None:
        paasta_cluster = superregion

    paasta_logs = PaaSTALogs(component, paasta_cluster, action_run_id)
    stream_name = paasta_logs.stream_name
    end_date: Optional[datetime.date]

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

    # S3LogsReader does not guarantee order of logs in the output - so we'll sort based on log timestamp set by producer.
    lines = paasta_logs.sorted_lines()
    malformed = (
        [f"{paasta_logs.malformed_lines} encountered while retrieving logs"] if paasta_logs.malformed_lines else []
    )

    truncation_message = (
        [
            f"This output is truncated. Use this command to view all lines: logreader -s {superregion} {stream_name} --min-date {min_date.date()} --max-date {max_date.date()} | jq --raw-output 'select(.tron_run_number=={int(paasta_logs.run_num)} and .component == \"{component}\") | .message'"
        ]
        if max_date
        else [
            f"This output is truncated. Use this command to view all lines: logreader -s {superregion} {stream_name} --min-date {min_date.date()} | jq --raw-output 'select(.tron_run_number=={int(paasta_logs.run_num)} and .component == \"{component}\") | .message'"
        ]
    )
    truncated = truncation_message if paasta_logs.truncated_output else []

    return lines + malformed + truncated
