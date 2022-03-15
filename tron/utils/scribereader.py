import datetime
import json
import logging
import operator
import shutil
import socket
import subprocess
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

    # NOTE: Passing in an ecosystem of prod is not supported by scribereader
    # as there's no mapping of ecosystem->scribe-kafka-services discovery hosts
    # for this ecosystem
    host, port = scribereader.get_tail_host_and_port(
        ecosystem=ecosystem if ecosystem != "prod" else None, region=region, superregion=superregion,
    )
    return host, port


def _(
    action_run_id: str,
    component: str,
    min_date: Optional[datetime.datetime],
    max_date: Optional[datetime.datetime],
    paasta_cluster: Optional[str],
    max_lines: Optional[int] = None,
):
    if min_date is None:
        return [f"{action_run_id} has not started yet."]

    if shutil.which("scribereader") is None:
        return ["Scribereader (an internal Yelp package) is not available - unable to display logs."]

    try:
        ecosystem = get_ecosystem()
        superregion = get_superregion()
    except OSError:
        return [
            "Unable to determine where Tron is located. If you're seeing this inside Yelp, report this to #compute-infra"
        ]

    selector = f"--superregion={superregion}" if ecosystem == "prod" else f"--ecosystem={ecosystem}"
    scribereader_args = [
        "scribereader",
        selector,
        f"--min-date={min_date.date()}",
    ]

    end_date = max_date.date() if max_date else None
    if end_date:
        scribereader_args.append(f"--max-date={end_date}")

    namespace, job_name, run_num, action = action_run_id.split(".")
    # in our logging infra, things are logged to per-instance streams - but
    # since Tron PaaSTA instances are of the form `job_name.action`, we need
    # to escape the period since some parts of our infra will reject streams
    # containing them - thus, the "weird" __ separator
    stream_name = f"stream_paasta_app_output_{namespace}_{job_name}__{action}"
    scribereader_args.append(stream_name)

    try:
        scribereader_process = subprocess.run(scribereader_args, stdout=subprocess.PIPE, check=True, encoding="utf-8")
    except subprocess.CalledProcessError as e:
        return [f"Unable to get logs from scribereader: {e}"]

    lines_filter = "[-{max_lines}:]" if max_lines else ""
    if paasta_cluster is None:
        paasta_cluster = get_superregion()

    jq_args = [
        "jq",
        # it's much easier to process things if we have the contents as a list of objects rather
        # than as a stream of objects
        "--slurp",
        f'[ .[] | select( \
            .tron_run_number=={run_num} \
            and .component == "{component}" \
            and .cluster == "{paasta_cluster}" \
         ) | .message]{lines_filter}',
    ]
    try:
        output_lines = subprocess.run(
            jq_args, stdout=subprocess.PIPE, input=scribereader_process.stdout, check=True, encoding="utf-8",
        )
    except subprocess.CalledProcessError as e:
        return [f"Unable to filter logs from scribereader: {e}"]

    return json.loads(output_lines.stdout)


def read_log_stream_for_action_run(
    action_run_id: str,
    component: str,
    min_date: Optional[datetime.datetime],
    max_date: Optional[datetime.datetime],
    paasta_cluster: Optional[str],
    max_lines: Optional[int] = None,
) -> List[str]:
    return _(
        action_run_id=action_run_id,
        component=component,
        min_date=min_date,
        max_date=max_date,
        paasta_cluster=paasta_cluster,
        max_lines=max_lines,
    )
