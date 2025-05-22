import subprocess

import pytest

from hpce_utils import shell


def test_subprocess_error():
    command_fails = "this_command_does_not_exist"
    with pytest.raises(subprocess.CalledProcessError):
        shell.execute(command_fails, check=True)


def test_subprocess_error_retry():
    command_fails = "this_command_does_not_exist"
    with pytest.raises(subprocess.CalledProcessError):
        shell.execute_with_retry(command_fails, max_retries=0)
