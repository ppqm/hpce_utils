import datetime
import logging
import os
import re
import time
from typing import Dict, Iterator, List, Optional, Tuple, Union

import pandas as pd  # type: ignore
from pandas import DataFrame  # type: ignore
from tqdm import tqdm  # type: ignore

from hpce_utils.shell import execute  # type: ignore

logger = logging.getLogger(__name__)

# tqdm default view
# TODO Put this somewhere more general
TQDM_OPTIONS = {
    "ncols": 95,
}

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


COLUMN_JOB = "job"
COLUMN_JOB_ID = "job_number"
COLUMN_SUBMISSION_TIME = "submission_time"
COLUMN_TASKARRAY = "job-array tasks"
#

class TaskarrayProgress:
    def __init__(
        self,
        pdf: DataFrame,
        job_id: str,
        job_info: Optional[Dict[str, str]] = None,
        position: int = 0,
    ) -> None:
        self.position = position
        self.job_id = str(job_id)

        # Get info

        if job_info is None:
            job_info = get_qstatj(job_id)

        if COLUMN_TASKARRAY not in job_info:
            raise ValueError("Not array task")

        # Only about this job
        _job_pdf = pdf[pdf[COLUMN_JOB] == self.job_id]
        job_status = dict(_job_pdf.iloc[0])
        self.init_bar(job_info, job_status)

    def init_bar(self, job_info: dict, job_status: dict) -> None:
        job_id = job_info[COLUMN_JOB_ID]
        # job_name = job_info["job_name"]
        start_time = job_info[COLUMN_SUBMISSION_TIME]
        array_info = job_info[COLUMN_TASKARRAY]

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

    def finish(self) -> None:
        n_total = self.n_total
        self.pbar.set_postfix({})
        self.pbar.set_description(f"{self.title} (0)", refresh=False)
        self.pbar.n = n_total
        self.pbar.refresh()

    def log_errors(self) -> None:

        qstatj = get_qstatj(self.job_id)
        errors = _get_errors_from_qstatj(qstatj)

        for error in errors:
            logger.error(f"uge {self.job_id}: {error.strip()}")

    def is_finished(self) -> bool:
        return self.pbar.n >= self.n_total

    def close(self) -> None:
        self.pbar.close()


def _get_qstatj_key(line: str) -> Tuple[Optional[str], str]:
    """Split column key and column value from qstat -j output"""

    # format:
    # start_time            2:    01/01/1970 01:00:00.000
    # job-array tasks:            1-3:1
    # error reason    1:          01/31/2024 17:05:39 [94281059:32991]: can't make directory

    col_key_end = 28

    # Does it have some pre-defined spaces
    spaces = line[col_key_end - 3 : col_key_end].strip()
    if len(spaces) > 0:
        return None, line

    if len(line) < col_key_end:
        return None, line

    key = line[:col_key_end]
    value = line[col_key_end:]

    # remove ":" colon
    key = key.strip()[:-1]

    if len(key) == 0:
        return None, line

    return key, value.strip()


def parse_qstatj(stdout: str) -> dict:
    """Parse the stdout of qstat -j into a dict"""

    out = dict()
    lines = stdout.split("\n")

    _key = None
    for line in lines[1:]:

        key, value = _get_qstatj_key(line)

        if key is None:
            key = _key
            value = "\n" + value

        _key = key

        if key not in out:
            out[key] = ""

        out[key] += value

    return out


def parse_qstat(stdout: str) -> pd.DataFrame:
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


def parse_taskarray(pdf: DataFrame) -> pd.DataFrame:
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

        row = {
            "job": job_id,
            "running": n_running,
            "pending": n_pending,
            "error": n_error,
        }

        rows.append(row)

    return pd.DataFrame(rows)


def parse_qacctj(stdout: str) -> List[Dict[str, str]]:
    output: List[Dict[str, str]] = [dict()]

    _stdout = stdout.split("\n")

    for line in _stdout:
        if "===========" in line:
            if len(output[-1]) > 1:
                output += [dict()]
            continue

        # Format: pe_taskid     NONE
        key = line[0:13].strip()
        value = line[13:].strip()

        if len(key) == 0:
            continue

        output[-1][key] = value

    return output


def _get_errors_from_qstatj(qstatj: Dict[str, str]) -> List[str]:
    key1 = "error reason   "
    error_keys = [key for key in qstatj.keys() if key1 in key]
    errors = [qstatj[key] for key in error_keys]
    return errors


def follow_progress(
    username: Optional[str] = None,
    job_ids: Optional[List[Union[int, str]]] = None,
    update_interval: int = 5,
    exit_after: Optional[int] = None,
) -> None:
    """Follow UGE jobs for $USER. All jobs or subset of job IDs.

    Current implementation only supports task-arrays.
    """

    if username is None:
        username = os.environ.get("USER", None)

    if username is None:
        raise ValueError("Unable to get USER env var")

    qstat = get_qstat(username)

    if not len(qstat):
        logger.warning(f"No jobs for {username}")
        return

    # TODO Add check that job_id is even valid
    if job_ids is None:
        job_ids = qstat["job"].unique()

    progresses = []

    for i, job_id in enumerate(job_ids):

        # Make sure that job_id is in qstat
        if str(job_id) not in qstat["job"].values:
            logger.warning(f"Job ID {job_id} not found in qstat. Skipping...")
            continue

        job = qstat.loc[qstat["job"] == str(job_id)]  # will return one result
        job = job.iloc[0]
        qstatj = get_qstatj(job_id)

        if job.running + job.pending == 0 and job.error > 0:
            # crashed job
            errors = _get_errors_from_qstatj(qstatj)
            logger.error(f"UGE job {job_id} is NOT starting for the following reason(s):")
            for error in errors:
                logger.error(error)

            continue

        if COLUMN_TASKARRAY not in qstatj:
            logger.info(f"Ignoring {job_id}, not a task-array")
            continue

        progress = TaskarrayProgress(qstat, str(job_id), job_info=qstatj, position=i)
        progresses.append(progress)

    # TODO Get status if jobs are finished or deleted
    # Then remove progressbars and return

    # TODO Check if job-array has errors, use logger.error to print them out. Maybe with a unique()

    # TODO While job_ids, then remove when not there anymore

    iterations = 0
    while True:
        iterations += 1

        if exit_after is not None and iterations > exit_after:
            break

        if len(progresses) == 0:
            break

        if all([bar.is_finished() for bar in progresses]):

            for bar in progresses:
                bar.log_errors()

            break

        time.sleep(update_interval)
        qstat = get_qstat(username)

        if len(qstat) == 0:
            for _, array_bar in enumerate(progresses):
                array_bar.finish()
            break

        # While jobs are not done
        finished = []
        for i, array_bar in enumerate(progresses):

            if array_bar.is_finished():
                continue

            # Get and update tatus
            job_info: DataFrame = qstat[qstat["job"] == array_bar.job_id]

            if len(job_info) == 0:
                array_bar.finish()
                finished.append(i)
                continue

            job_status = dict(job_info.iloc[0])
            array_bar.update(job_status)

    for bar in progresses:
        bar.close()

    return


def get_qstatj(job_id: Union[str, int]) -> Dict[str, str]:
    """Get job information"""
    stdout, _ = execute(f"qstat -j {job_id} | head -n 100")
    return parse_qstatj(stdout)


def get_qstat(username: str) -> pd.DataFrame:
    """Get job information for user"""
    stdout, _ = execute(f"qstat -u {username}")

    if stdout is None or len(stdout) == 0:
        return pd.DataFrame({})

    pdf = parse_qstat(stdout)
    pdf_ = parse_taskarray(pdf)
    return pdf_


def get_qacctj(job_id: Union[str, int]) -> pd.DataFrame:
    """Get detailed job information"""

    stdout, _ = execute(f"qacct -j {job_id}")

    if stdout is None or len(stdout) == 0:
        return pd.DataFrame({})

    output = parse_qacctj(stdout)

    pdf = pd.DataFrame(output)

    return pdf


def get_cluster_usage() -> DataFrame:
    """Get cluster usage information, grouped by users

    To get totla cores in use `pdf["slots"].sum()`
    """

    stdout, _ = execute("qstat -u \\*")  # noqa: W605
    pdf = parse_qstat(stdout)

    # filter to running
    pdf = pdf[pdf.state.isin(running_tags)]

    counts = pdf.groupby(["user"])["slots"].agg("sum")
    counts = counts.sort_values()  # type: ignore

    return counts


def wait_for_jobs(
    jobs: List[str], respiratory: int = 60, include_status: bool = True
) -> Iterator[str]:
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
    still_waiting_states = pending_tags + running_tags

    status_j = get_qstatj(job_id)

    if not len(status_j):
        return True

    state = status_j.get("job_state", "qw")
    logger.debug(f"uge {job_id} is {state}")

    if state not in still_waiting_states:
        return True

    return False
