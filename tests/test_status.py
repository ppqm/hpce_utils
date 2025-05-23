import os
from pathlib import Path
from subprocess import CalledProcessError as CPError
from unittest.mock import MagicMock, patch

import pytest

from hpce_utils.managers.uge import status, submitting

VALID_QSTAT_OUTPUT_RUNNING = """
job-ID     prior   name       user         state submit/start at     queue                          jclass                         slots ja-task-ID
---------------------------------------------------------------------------------
12345678   0.5019  job        username     r     05/19/2025 09:18:15 some.q@some.server.eu.                                        1     1
"""
QSTATJ_OUTPUT = """
job_number:                 26936216
submission_time:            05/19/2025 13:37:07.436
job-array tasks:            0-1
"""
VALID_QSTAT_OUTPUT_FINISHED = ""
QSTATJ_OUTPUT_FINISHED = ""
QACCTJ_OUTPUT_FINISHED = """
==============================================================
qname                    some.q
hostname                 some.host
group                    some_group
owner                    username
project                  NONE
department               some_department
jobname                  some_name
jobnumber                12345678
taskid                   1
pe_taskid                NONE
"""


def test_qstat_error():
    with patch("hpce_utils.shell.subprocess.run") as mock_subprocess:
        mock_subprocess.side_effect = CPError("fail", 1)
        with pytest.raises(CPError):
            status.get_qstat("username", max_retries=0)


def test_follow_progress():
    # Prepare mock process objects
    mock_proc_running = MagicMock()
    mock_proc_running.stdout = VALID_QSTAT_OUTPUT_RUNNING
    mock_proc_running.stderr = ""
    mock_proc_running.returncode = 0

    mock_proc_qstatj = MagicMock()
    mock_proc_qstatj.stdout = QSTATJ_OUTPUT
    mock_proc_qstatj.stderr = ""
    mock_proc_qstatj.returncode = 0

    mock_proc_finished = MagicMock()
    mock_proc_finished.stdout = VALID_QSTAT_OUTPUT_FINISHED
    mock_proc_finished.stderr = ""
    mock_proc_finished.returncode = 0

    qstatj_finished_error = CPError(
        cmd="qstat -j 12345678",
        returncode=1,
        stderr="Following jobs do not exist or permissions are not sufficient: 12345678",
        output="",
    )

    qacctj_finished = MagicMock()
    qacctj_finished.stdout = QACCTJ_OUTPUT_FINISHED
    qacctj_finished.stderr = ""
    qacctj_finished.returncode = 0

    side_effects = [
        mock_proc_running,
        mock_proc_qstatj,
        mock_proc_running,
        mock_proc_finished,
        qstatj_finished_error,
        qacctj_finished,
        qstatj_finished_error,  # for array_bar.log_errors()
    ]

    # Patch subprocess.run
    with patch("hpce_utils.shell.subprocess.run", side_effect=side_effects) as mock_subprocess:
        # Patch tqdm to avoid actual progress bars in test output
        with patch("hpce_utils.managers.uge.status.tqdm"):
            status.follow_progress(
                username="username",
                job_ids=["12345678"],
                update_interval=0.1,
            )

        assert mock_subprocess.call_count == 7


def test_follow_progress_with_qstat_failures(caplog):
    # Simulate: success, success, failure, success, finished

    # Prepare mock process objects
    mock_proc_running = MagicMock()
    mock_proc_running.stdout = VALID_QSTAT_OUTPUT_RUNNING
    mock_proc_running.stderr = ""
    mock_proc_running.returncode = 0

    mock_proc_qstatj = MagicMock()
    mock_proc_qstatj.stdout = QSTATJ_OUTPUT
    mock_proc_qstatj.stderr = ""
    mock_proc_qstatj.returncode = 0

    mock_proc_finished = MagicMock()
    mock_proc_finished.stdout = VALID_QSTAT_OUTPUT_FINISHED
    mock_proc_finished.stderr = ""
    mock_proc_finished.returncode = 0

    qstat_error = CPError(
        cmd="qstat",
        returncode=1,
        stderr="qstat error",
        output="",
    )

    qstatj_finished_error = CPError(
        cmd="qstat -j 12345678",
        returncode=1,
        stderr="Following jobs do not exist or permissions are not sufficient: 12345678",
        output="",
    )

    qacctj_finished = MagicMock()
    qacctj_finished.stdout = QACCTJ_OUTPUT_FINISHED
    qacctj_finished.stderr = ""
    qacctj_finished.returncode = 0

    # The sequence: running, qtstat -j, running, error, running, finished
    side_effects = [
        mock_proc_running,
        mock_proc_qstatj,
        mock_proc_running,
        qstat_error,
        mock_proc_running,
        mock_proc_finished,
        qstatj_finished_error,
        qacctj_finished,
        qstatj_finished_error,  # for array_bar.log_errors()
    ]

    # Patch subprocess.run in the correct module
    with patch("hpce_utils.shell.subprocess.run", side_effect=side_effects) as mock_subprocess:
        # Patch tqdm to avoid actual progress bars in test output
        with patch("hpce_utils.managers.uge.status.tqdm"):
            with caplog.at_level("WARNING", logger="hpce_utils.managers.uge.status"):
                status.follow_progress(
                    username="username", job_ids=["12345678"], update_interval=0.1
                )

        assert mock_subprocess.call_count == 9

    # Check that the log contains the expected messages
    all_logs = caplog.text
    assert "stdout" in all_logs
    assert "stderr" in all_logs
    assert "returncode" in all_logs


def test_follow_progress_with_initial_qstat_failures(caplog):
    # Simulate: success, success, failure, success, finished

    # Prepare mock process objects
    mock_proc_running = MagicMock()
    mock_proc_running.stdout = VALID_QSTAT_OUTPUT_RUNNING
    mock_proc_running.stderr = ""
    mock_proc_running.returncode = 0

    mock_proc_qstatj = MagicMock()
    mock_proc_qstatj.stdout = QSTATJ_OUTPUT
    mock_proc_qstatj.stderr = ""
    mock_proc_qstatj.returncode = 0

    mock_proc_finished = MagicMock()
    mock_proc_finished.stdout = VALID_QSTAT_OUTPUT_FINISHED
    mock_proc_finished.stderr = ""
    mock_proc_finished.returncode = 0

    qstat_error = CPError(
        cmd="qstat",
        returncode=1,
        stderr="qstat error",
        output="",
    )

    qstatj_finished_error = CPError(
        cmd="qstat -j 12345678",
        returncode=1,
        stderr="Following jobs do not exist or permissions are not sufficient: 12345678",
        output="",
    )

    qacctj_finished = MagicMock()
    qacctj_finished.stdout = QACCTJ_OUTPUT_FINISHED
    qacctj_finished.stderr = ""
    qacctj_finished.returncode = 0

    # The sequence: error, error, running, qstat -j, running, error, running, finished
    side_effects = [
        qstat_error,
        qstat_error,
        mock_proc_running,
        mock_proc_qstatj,
        mock_proc_running,
        qstat_error,
        mock_proc_running,
        mock_proc_finished,
        qstatj_finished_error,
        qacctj_finished,
        qstatj_finished_error,  # for array_bar.log_errors()
    ]

    # Patch subprocess.run in the correct module
    with patch("hpce_utils.shell.subprocess.run", side_effect=side_effects) as mock_subprocess:
        # Patch tqdm to avoid actual progress bars in test output
        with patch("hpce_utils.managers.uge.status.tqdm"):
            with caplog.at_level("WARNING", logger="hpce_utils.managers.uge.status"):
                status.follow_progress(
                    username="username", job_ids=["12345678"], update_interval=0.1, exit_after=5
                )

        assert mock_subprocess.call_count == 11

    # Check that the log contains the expected messages
    all_logs = caplog.text
    assert "stdout" in all_logs
    assert "stderr" in all_logs
    assert "returncode" in all_logs


def test_wait_for_jobs_without_qstat(home_tmp_path: Path):
    print("scratch:", home_tmp_path)
    log_dir = home_tmp_path / "uge_testlogs"
    script_1: str = submitting.generate_taskarray_script(
        "sleep 10",
        cores=1,
        cwd=home_tmp_path,
        log_dir=log_dir,
        name="TestJob",
        task_concurrent=1,
        task_stop=1,
    )

    script_2: str = submitting.generate_taskarray_script(
        "sleep 9",
        cores=1,
        cwd=home_tmp_path,
        log_dir=log_dir,
        name="TestJob2",
        task_concurrent=1,
        task_stop=1,
    )

    job_id_1, _ = submitting.submit_script(script_1, scr=home_tmp_path)
    job_id_2, _ = submitting.submit_script(script_2, scr=home_tmp_path)
    assert job_id_1 is not None
    assert job_id_2 is not None

    print(job_id_1, job_id_2)

    finished_file = status.wait_for_jobs_without_qstat(
        [job_id_1, job_id_2],
        scr=home_tmp_path,
        log_dir=log_dir,
        update_interval=5,
    )

    assert finished_file.exists()

    username = os.getenv("USER")
    assert username is not None
    qstat, log_str = status.get_qstat(username, max_retries=0)
    print(log_str)

    assert job_id_1 not in qstat["job"]
    assert job_id_2 not in qstat["job"]
