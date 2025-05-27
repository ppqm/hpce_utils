import logging
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


def command_exists(cmd):
    """Does this command even exists?"""

    path = shutil.which(cmd)

    if path is None:
        return False

    return True


def switch_workdir(path: Optional[Path]) -> bool:
    """Check if it makes sense to change directory"""

    if path is None:
        return False

    if path == "":
        return False

    if path == "./":
        return False

    if path == ".":
        return False

    assert os.path.exists(path), f"Cannot change directory, does not exists {path}"

    return True


def stream(cmd: str, cwd: Optional[Path] = None, shell: bool = True):
    """Execute command in directory, and stream stdout. Last yield is
    stderr

    :param cmd: The shell command
    :param cwd: Change directory to work directory
    :param shell: Use shell or not in subprocess
    :param timeout: Stop the process at timeout (seconds)
    :returns: Generator of stdout lines. Last yield is stderr.
    """

    if not switch_workdir(cwd):
        cwd = None

    popen = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        shell=shell,
        cwd=cwd,
    )

    for stdout_line in iter(popen.stdout.readline, ""):  # type: ignore
        yield stdout_line

    # Yield errors
    stderr = popen.stderr.read()  # type: ignore
    popen.stdout.close()  # type: ignore
    yield stderr

    return


def execute(
    cmd: str,
    cwd: Optional[Path] = None,
    shell: bool = True,
    timeout: None = None,
    check: bool = True,
) -> Tuple[str, str]:
    """Execute command in directory, and return stdout and stderr

    :param cmd: The shell command
    :param cwd: Change directory to work directory
    :param shell: Use shell or not in subprocess
    :param timeout: Stop the process at timeout (seconds)
    :returns: stdout and stderr as string
    :raises: subprocess.CalledProcessError if check is True and command fails
    :raises: subprocess.TimeoutExpired if timeout is reached
    :raises: FileNotFoundError if check is True and command is not found
    """

    if not switch_workdir(cwd):
        cwd = None

    try:
        process = subprocess.run(
            cmd,
            cwd=cwd,
            encoding="utf-8",
            shell=shell,
            check=check,
            capture_output=True,
            timeout=timeout,
        )
    except subprocess.CalledProcessError as exc:
        logger.error("Command %s failed", cmd)
        logger.error("stdout: %s", exc.stdout)
        logger.error("stderr: %s", exc.stderr)
        logger.error("returncode: %s", exc.returncode)
        raise exc

    except FileNotFoundError as exc:
        logger.error("Command %s not found:", cmd)
        if check:
            raise exc
        else:
            return "", ""

    except subprocess.TimeoutExpired as exc:
        logger.error("Command %s timed out:", cmd)
        if check:
            raise exc
        else:
            stderr = "" if exc.stderr is None else exc.stderr.decode("utf-8")
            stdout = "" if exc.stdout is None else exc.stdout.decode("utf-8")

            return stdout, stderr

    return process.stdout, process.stderr


def execute_with_retry(
    cmd: str,
    cwd: Optional[Path] = None,
    shell: bool = True,
    timeout: None = None,
    max_retries: int = 3,
    update_interval: int = 5,
) -> Tuple[str, str]:
    """Execute command in directory, and return stdout and stderr

    :param cmd: The shell command
    :param cwd: Change directory to work directory
    :param shell: Use shell or not in subprocess
    :param timeout: Stop the process at timeout (seconds)
    :param max_retries: How many times to rerun the command before raising an error
    :param update_interval: How long to wait between retries
    :returns: stdout and stderr as string
    :raises: subprocess.CalledProcessError if command fails more than max_retries
    :raises: subprocess.TimeoutExpired if timeout is reached and the command failed more than max_retries
    :raises: FileNotFoundError if command is not found
    """

    num_retries = 0
    while True:
        try:
            stdout, stderr = execute(cmd, cwd=cwd, shell=shell, timeout=timeout, check=True)
            return stdout, stderr
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError) as exc:
            logger.warning("Error while executing %s. Try again later.", cmd)
            time.sleep(update_interval)
            if num_retries >= max_retries:
                logger.error("Max retries reached for command %s", cmd)
                raise exc
            num_retries += 1


def source(bashfile):
    """
    Return resulting environment variables from sourceing a bashfile

    usage:
        env_dict = source("/path/to/aws_cred_ang")
        os.environ.update(env_dict)

    :returns: dict of variables
    """

    cmd = f'env -i sh -c "source {bashfile} && env"'
    stdout, _ = execute(cmd)
    lines = stdout.split("\n")

    variables = dict()

    for line in lines:

        line = line.split("=")

        # Ignore wrong lines
        # - empty
        # - multiple =
        if len(line) != 2:
            continue

        key, var = line

        if key == "PWD":
            continue

        if key == "_":
            continue

        if key == "SHLVL":
            continue

        variables[key] = var

    return variables


def directory_add_trail(path: str) -> str:
    """ """

    # Remove trailing spaces
    path = path.strip()

    if path == "":
        return path

    if not path.endswith(os.path.sep):
        path += os.path.sep

    return path


def directory_remove_trail(path: str) -> str:

    # Remove trailing spaces
    path = path.strip()

    if path.endswith(os.path.sep):
        path = path[:-1]

    return path


def get_environment(env_names: List[str]) -> Dict[str, str]:
    """Get environ variables that matter"""
    environ = dict()

    for var in env_names:
        value = os.environ.get(var, None)

        if value:
            environ[var] = value

    return environ


def which(cmd: Union[Path, str]) -> Optional[Path]:
    """Check if command exists in enviroment"""
    path_ = shutil.which(cmd)

    if path_ is None:
        return None

    path = Path(path_)

    return path
