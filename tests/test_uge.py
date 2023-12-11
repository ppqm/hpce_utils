import pytest

from hpce_utils.queues import uge
from hpce_utils.queues.uge import status, submitting

if not uge.has_uge():
    pytest.skip("Could not find UGE executable", allow_module_level=True)


def test_generate_submit_script():
    command = "which python"

    options = dict()

    script: str = submitting.generate_taskarray_script(command, **options)
    print(script)

    assert command in script


def test_taskarray(tmp_path):

    # Generate script
    success_string = "finished work"
    command = f"sleep 5 & echo '{success_string}'"
    log_dir = "./uge_testlogs/"
    options = dict(
        cores=1,
        task_stop=2,
        task_concurrent=1,
        log_dir=log_dir,
    )
    script: str = submitting.generate_taskarray_script(command, **options)
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


def test_submit_single_job():

    return


def test_failed_job():

    return
