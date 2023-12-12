import os
import time
from pathlib import Path

import pytest

from hpce_utils.queues import uge
from hpce_utils.queues.uge import status, submitting
from hpce_utils.queues.uge.constants import UGE_TASK_ID

if not uge.has_uge():
    pytest.skip("Could not find UGE executable", allow_module_level=True)


def test_generate_submit_script():
    command = "which python"

    script: str = submitting.generate_taskarray_script(command)
    print(script)

    assert command in script


def test_taskarray(tmp_path):

    # Generate script
    success_string = "finished work"
    command = f"sleep 5 & echo '{success_string}'"
    log_dir = "./uge_testlogs/"
    script: str = submitting.generate_taskarray_script(
        command,
        cores=1,
        task_stop=2,
        task_concurrent=1,
        log_dir=log_dir,
    )
    print(script)
    assert command in script

    # Submit script
    print("scratch:", tmp_path)
    submit_options = dict(
        scr=tmp_path,
        # dry=True,
    )
    job_id = submitting.submit_job(script, **submit_options)
    print(job_id)
    assert job_id is not None

    # Wait
    finished_job_id: str | None = None
    for finished_job_id in status.wait_for_jobs([job_id], respiratory=1):
        print(f"job {finished_job_id} finished")
        finished_job_id = finished_job_id

    assert finished_job_id is not None
    stdout, stderr = submitting.parse_logfiles(
        tmp_path / log_dir, finished_job_id, ignore_stdout=False
    )

    print(stdout)
    print(stderr)

    assert len(stderr) == 0
    assert len(stdout) == 2

    # Parse output
    for line in stdout:
        assert success_string in line


def test_reconstruct_env():
    # Set env variable
    env_var_name = "GREETINGSVAR"
    env_var_value = "HELLO WORLD"
    os.environ[env_var_name] = env_var_value

    # Submit keeping the env variable

    # Check it is printed out


def test_submit_single_job():
    pass


def test_failed_job(tmp_path: Path):

    # Generate script
    # Command fails on the 2nd task
    # command = f"test \"{UGE_TASK_ID}\" == 2 && command_does_not_exist || pwd"
    command = (
        f'set -e; sleep 2; if test "{UGE_TASK_ID}" == 2; then command_does_not_exist; else pwd; fi'
    )
    log_dir = "./uge_testlogs/"
    n_tasks = 2
    script: str = submitting.generate_taskarray_script(
        command,
        cores=1,
        task_stop=n_tasks,
        task_concurrent=1,
        log_dir=log_dir,
    )
    print(script)
    assert command in script

    # Submit script
    print("scratch:", tmp_path)
    job_id = submitting.submit_job(
        script,
        scr=tmp_path,
        # dry=True,
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
    stdout, stderr = submitting.parse_logfiles(tmp_path / log_dir, job_id, ignore_stdout=False)
    print("stdout:", stdout)
    print("stderr:", stderr)
    assert len(stderr) == 1
    assert len(stdout) == 1
