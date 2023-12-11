import datetime
import logging
import os
import re
import subprocess
import time
from typing import List, Optional, Tuple, Union

import numpy as np
import pandas as pd  # type: ignore
from pandas import DataFrame  # type: ignore
from tqdm import tqdm  # type: ignore

from hpce_utils.shell import execute  # type: ignore

logger = logging.getLogger(__name__)

# tqdm default view
TQDM_OPTIONS = {
    "ncols": 95,
}

# if run from jupyter, print to stdout and not stderr
# if env.is_notebook():
#     TQDM_OPTIONS["file"] = sys.stdout

"""
| **Category**  | **State**                                      | **SGE Letter Code** |
| ------------- |:-----------------------------------------------| :------------------ |
| Pending       | pending                                        | qw                  |
| Pending       | pending, user hold                             | qw                  |
| Pending       | pending, system hold                           | hqw                 |
| Pending       | pending, user and system hold                  | hqw                 |
| Pending       | pending, user hold, re-queue                   | hRwq                |
| Pending       | pending, system hold, re-queue                 | hRwq                |
| Pending       | pending, user and system hold, re-queue        | hRwq                |
| Pending       | pending, user hold                             | qw                  |
| Pending       | pending, user hold                             | qw                  |
| Running       | running                                        | r                   |
| Running       | transferring                                   | t                   |
| Running       | running, re-submit                             | Rr                  |
| Running       | transferring, re-submit                        | Rt                  |
| Suspended     | obsuspended                                    | s,  ts              |
| Suspended     | queue suspended                                | S, tS               |
| Suspended     | queue suspended by alarm                       | T, tT               |
| Suspended     | allsuspended withre-submit                     | Rs,Rts,RS, RtS, RT, RtT |
| Error         | allpending states with error                   | Eqw, Ehqw, EhRqw        |
| Deleted       | all running and suspended states with deletion | dr,dt,dRr,dRt,ds, dS, dT,dRs, dRS, dRT |
"""

pending_tags = ["qw", "hqw", "hRwq"]
running_tags = ["r", "t", "Rr", "Rt", "x"]
suspended_tags = "s,ts,S,tS,T,tT,Rs,Rts,RS,RtS,RT,RtT".split(",")
error_tags = "Eqw,Ehqw,EhRqw".split(",")
deleted_tags = "dr,dt,dRr,dRt,ds,dS,dT,dRs,dRS,dRT".split(",")


KEY_TASKARRAY = "job-array_tasks"


class TaskarrayProgress:
    def __init__(self, pdf, job_id, job_info=None, position=0) -> None:
        self.position = position
        self.job_id = job_id

        # Get info

        if job_info is None:
            job_info = get_qstatj(job_id)

        if KEY_TASKARRAY not in job_info:
            raise ValueError("Not array task")

        # Only about this job
        job_status = dict(pdf[pdf["job"] == self.job_id].iloc[0])

        self.init_bar(job_info, job_status)

    def init_bar(self, job_info: dict, job_status: dict) -> None:

        job_id = job_info["job_number"]
        # job_name = job_info["job_name"]
        start_time = job_info["submission_time"]
        array_info = job_info["job-array_tasks"]

        # Collect and parse submission time
        start_time = ".".join(start_time.split(".")[:-1])
        date_format = "%m/%d/%Y %H:%M:%S"
        start_time_ = datetime.datetime.strptime(start_time, date_format)
        start_time__ = time.mktime(start_time_.timetuple())

        # Count total tasks
        n_total_ = array_info.split(":")[0].split("-")[-1]
        self.n_total = int(n_total_)

        # Set title
        self.title = job_id

        self.pbar = tqdm(
            total=self.n_total,
            desc=f"{self.title}",
            position=self.position,
            **TQDM_OPTIONS,
        )

        # Reset time
        self.pbar.last_print_t = self.pbar.start_t = start_time__

        # Set finished and running
        self.update(job_status)

    def update(self, status: dict) -> None:

        n_running = status.get("running", 0)
        n_pending = status.get("pending", 0)
        n_error = status.get("error", 0)
        n_finished = self.n_total - n_pending - n_running

        postfix = dict()

        if n_error > 0:
            postfix["err"] = n_error

        self.pbar.set_postfix(postfix)

        self.pbar.set_description(f"{self.title} ({n_running})", refresh=False)
        self.pbar.n = n_finished
        self.pbar.refresh()

    def close(self):
        self.pbar.close()


def parse_qstatj(stdout) -> dict:

    out = dict()
    lines = stdout.split("\n")

    for line in lines[1:]:
        try:
            key, value = line.split(":", 1)
        except Exception:
            continue
        key = key.strip().replace(" ", "_")
        value = value.strip()
        out[key] = value

    return out


def parse_qstat(stdout) -> pd.DataFrame:

    stdout = stdout.strip()
    lines = stdout.split("\n")

    header = lines[0].split()
    header.remove("at")
    header_indicies = []

    rows = []

    for line in header[1:]:
        idx = lines[0].index(line)
        header_indicies.append(idx)

    def split_qstat_line(line):

        idx = 0

        for ind in header_indicies:
            yield line[idx:ind].strip()
            idx = ind

        yield line[idx:]

    for line in lines[2:]:

        if not line.strip():
            continue

        line_ = split_qstat_line(line)
        line_ = list(line_)

        row = {key: value for key, value in zip(header, line_)}
        rows.append(row)

    pdf = pd.DataFrame(rows)
    pdf["slots"] = pdf["slots"].astype(int)

    return pdf


def parse_taskarray(pdf) -> pd.DataFrame:

    col_id = "job-ID"
    col_state = "state"
    col_array = "ja-task-ID"

    # for unique job-ids
    job_ids = pdf[col_id].unique()

    def _parse(line):

        count = 0

        lines = line.split(",")
        for task in lines:
            if "-" not in task:
                count += 1
                continue

            start, stop, _ = re.split(",|:|-|!", task)
            count += int(stop) - int(start)

        return count

    rows = []

    for job_id in job_ids:

        # TODO Find all unique states
        # - running
        # - pending
        # - error

        jobs = pdf[pdf[col_id] == job_id]

        pending_jobs = jobs[jobs[col_state].isin(pending_tags)]
        running_jobs = jobs[jobs[col_state].isin(running_tags)]
        error_jobs = jobs[jobs[col_state].isin(error_tags)]
        # deleted_jobs = jobs[jobs[col_state].isin(deleted_tags)]

        pending_count = pending_jobs[col_array].apply(_parse)
        error_count = error_jobs[col_array].apply(_parse)

        n_running = len(running_jobs)
        n_pending = pending_count.values.sum()
        n_error = error_count.values.sum()

        row = {"job": job_id, "running": n_running, "pending": n_pending, "error": n_error}

        rows.append(row)

    return pd.DataFrame(rows)


def run(cmd: str) -> Tuple[str, str]:

    with subprocess.Popen(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as popen:

        bstdout, bstderr = popen.communicate()

        stdout: str = bstdout.decode("utf-8")
        stderr: str = bstderr.decode("utf-8")

    return stdout, stderr


def follow_progress(username=os.environ.get("USER"), job_id=Optional[str]) -> None:

    qstat = get_qstat(username)

    if len(qstat) == 0:
        print(f"No jobs for {username}")
        return

    job_ids: Union[List, np.ndarray]

    if job_id is None:
        job_ids = qstat["job"].unique()
    else:
        job_ids = [job_id]

    progresses = []
    i = 0

    for job_id in job_ids:

        job_info = get_qstatj(job_id)
        if KEY_TASKARRAY not in job_info:
            continue

        progress = TaskarrayProgress(qstat, job_id, job_info=job_info, position=i)
        progresses.append(progress)
        #
        i += 1

    # TODO Get status if jobs are finished or deleted
    # Then remove progressbars and return

    for _ in range(5):
        time.sleep(5)

        qstat = get_qstat(username)

        # While jobs are not done
        for _, array_bar in enumerate(progresses):

            # Get and update tatus
            job_status = dict(qstat[qstat["job"] == array_bar.job_id].iloc[0])
            array_bar.update(job_status)

    return


def get_qstatj(job_id: str) -> dict:
    stdout, _ = run(f"qstat -j {job_id} | head -n 100")
    return parse_qstatj(stdout)


def get_qstat(username: str) -> pd.DataFrame:
    stdout, _ = run(f"qstat -u {username}")

    if len(stdout) == 0:
        return pd.DataFrame({})

    pdf = parse_qstat(stdout)
    pdf_ = parse_taskarray(pdf)
    return pdf_


def get_usage() -> DataFrame:

    stdout, _ = run("qstat -u \\*")  # noqa: W605
    pdf = parse_qstat(stdout)

    # filter to running
    pdf = pdf[pdf.state.isin(running_tags)]

    # total_in_use = pdf["slots"].sum()

    counts = pdf.groupby(["user"])["slots"].agg("sum")
    counts = counts.sort_values()  # type: ignore

    # print(counts)
    # print(f"Total cores used: {total_in_use}")
    return counts


def wait_for_jobs(jobs: list[str], respiratory: int = 60, include_status=True):
    """ """

    logger.info(f"Waiting for {len(jobs)} job(s) on UGE...")

    start_time = time.time()

    while len(jobs):

        logger.info(
            f"... and breathe for {respiratory} sec, still waiting for {len(jobs)} job(s) to finish..."
        )

        time.sleep(respiratory)

        for job_id in jobs:

            if _uge_is_job_done(job_id):
                yield job_id
                jobs.remove(job_id)

    end_time = time.time()
    diff_time = end_time - start_time
    logger.info(f"All jobs finished and took {diff_time/60/60:.2f}h")


def _uge_is_job_done(job_id: str) -> bool:

    # still_waiting_states = ["q", "r", "qw", "dr", 'x', 't']
    still_waiting_states = pending_tags + running_tags

    status = get_status(job_id)

    if status is None:
        return True

    state = status.get("job_state", "qw")
    logger.debug(f"uge {job_id} is {state}")

    if state not in still_waiting_states:
        return True

    return False


def get_status(job_id: str | int) -> dict[str, str] | None:
    """
    Get status of SGE Job
    job_state
        q - queued
        qw - queued wait ?
        r - running
        dr - deleting
        dx - deleted
    job_name
    """

    cmd = f"qstat -j {job_id}"
    # TODO Check syntax for task-array

    stdout, stderr = execute(cmd)

    if stdout is None:
        stdout = ""

    if stderr is None:
        stderr = ""

    stderr = stderr.replace("\n", "")
    stderr = stderr.strip().rstrip()

    if stderr:
        logger.debug(stderr)
        return None

    lines = stdout.split("\n")

    status_ = dict()

    for line in lines:

        line_ = line.split(":")
        if len(line_) < 2:
            continue

        print(line)

        # TODO Is probably task_array related and needs are more general fix
        # job_state             1:    r
        key = line_[0]
        key = key.replace("    1", "")
        key = key.strip()

        content = line_[1].strip()

        status_[key] = content

    return status_
