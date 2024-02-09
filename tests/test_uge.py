import logging
import os
import time
from pathlib import Path

import pytest

from hpce_utils.managers import uge
from hpce_utils.managers.uge import status, submitting
from hpce_utils.managers.uge.constants import UGE_TASK_ID

if not uge.has_uge():
    pytest.skip("Could not find UGE executable", allow_module_level=True)


def test_generate_submit_script():
    command = "which python"
    script: str = submitting.generate_taskarray_script(command)
    print(script)
    assert command in script


def test_taskarray(home_tmp_path: Path):
    """Create a task-array job"""

    # Need network accessible folder
    tmp_path = home_tmp_path

    # Generate script
    success_string = "finished work"
    command = f"sleep 5 & echo '{success_string}'"
    log_dir = tmp_path / "uge_testlogs"
    script: str = submitting.generate_taskarray_script(
        command,
        cores=1,
        cwd=tmp_path,
        log_dir=log_dir,
        name="TestJob",
        task_concurrent=1,
        task_stop=2,
    )
    print(script)
    assert command in script

    # Submit script
    print("scratch:", tmp_path)
    job_id, _ = submitting.submit_script(script, scr=tmp_path)
    print(job_id)
    assert job_id is not None

    # Wait
    finished_job_id: str | None = None
    for finished_job_id in status.wait_for_jobs([job_id], respiratory=1):
        print(f"job {finished_job_id} finished")
        finished_job_id = finished_job_id

    assert finished_job_id is not None
    stdout, stderr = submitting.read_logfiles(log_dir, finished_job_id, ignore_stdout=False)

    print(stdout)
    print(stderr)

    assert len(stderr) == 0
    assert len(stdout) == 2

    # Parse output
    for _, line in stdout.items():
        assert success_string in line


@pytest.mark.skip(reason="Not implemented")
def test_reconstruct_env():
    # Set env variable
    env_var_name = "GREETINGSVAR"
    env_var_value = "HELLO WORLD"
    os.environ[env_var_name] = env_var_value

    # Submit keeping the env variable

    # Check it is printed out


@pytest.mark.skip(reason="Not implemented")
def test_submit_single_job():
    pass


def test_failed_command(home_tmp_path: Path):

    # Need network accessible folder
    tmp_path = home_tmp_path

    # Generate script
    # Command fails on the 2nd task
    command = (
        f'set -e; sleep 1; if test "{UGE_TASK_ID}" == 2; then command_does_not_exist; else pwd; fi'
    )
    log_dir = tmp_path / "logs"
    n_tasks = 2
    script: str = submitting.generate_taskarray_script(
        command,
        cores=1,
        log_dir=log_dir,
        name="TestJob",
        task_concurrent=1,
        task_stop=n_tasks,
    )
    print(script)
    assert command in script

    # Submit script
    print("scratch:", tmp_path)
    job_id, _ = submitting.submit_script(
        script,
        scr=tmp_path,
    )
    print(job_id)
    assert job_id is not None

    # Wait
    finished_job_id: str | None = None
    for finished_job_id in status.wait_for_jobs([job_id], respiratory=5):
        print(f"job {finished_job_id} finished")
        assert finished_job_id is not None

    # Parse results
    time.sleep(5)  # Wait for UGE to collect the result

    # Overview of the tasks
    pdf_report = status.get_qacctj(job_id)
    print(pdf_report)
    print(pdf_report["exit_status"])
    assert len(pdf_report) == n_tasks, "Missing tasks"
    pdf_report["exit_status"] = pdf_report["exit_status"].astype(int)

    assert max(pdf_report["exit_status"]) == 127, "Exit code mismatch"
    assert min(pdf_report["exit_status"]) == 0, "Exit code mismatch"

    # Get the log files
    stdout, stderr = submitting.read_logfiles(log_dir, job_id, ignore_stdout=False)
    print("stdout:", stdout)
    print("stderr:", stderr)
    assert len(stderr) == 1
    assert len(stdout) == 1


def test_failed_uge_submit(tmp_path: Path, caplog):
    """
    test failed job where uge cannot write to the log folder
    Expect the error to be in the uge logger
    """

    # Set a log path that does not exist, so the job fails
    log_dir = Path("/this/path/does/not/exist/")
    command = "echo 1"
    n_tasks = 3

    script: str = submitting.generate_taskarray_script(
        command,
        cores=1,
        generate_dirs=False,
        log_dir=log_dir,
        name="TestJob",
        task_concurrent=1,
        task_stop=n_tasks,
    )

    job_id, _ = submitting.submit_script(
        script,
        scr=tmp_path,
    )

    assert job_id is not None
    print(job_id)

    with caplog.at_level(logging.WARNING):
        status.follow_progress(job_ids=[job_id])

    error_line = 'can\'t make directory "/this/path/does/not" as stdout_path: Permission denied'
    print(caplog.text)
    assert error_line in caplog.text

    # Clean the bad job
    submitting.delete_job(job_id)
