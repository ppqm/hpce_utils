import logging
import os
import subprocess
import sys
from functools import cache
from pathlib import Path
from typing import Any, List, Tuple

from hpce_utils.shell import which

_logger = logging.getLogger("lmod")


@cache
def get_lmod_executable() -> Path | None:
    _dir = os.environ.get("LMOD_DIR", None)
    dir = Path(_dir) if _dir is not None else None
    exe = dir / "lmod" if dir is not None else None

    # TODO Raise exception if cannot find executable

    if exe is None:
        _logger.error("Could not find LMOD executable in environment")
        return None

    if not which(exe):
        _logger.error(f"Could not find {exe} executable in environment")
        return None

    return exe


# pylint: disable=too-many-locals
def module(command: str, arguments: str, cmd: Path | None = get_lmod_executable()) -> str | None:
    """Use lmod to execute enviromental changes"""

    _logger.info(f"module {command} {arguments}")

    execution: Any = [cmd, "python", command, arguments]

    _logger.debug(execution)

    with subprocess.Popen(
        execution,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as popen:

        bstdout, bstderr = popen.communicate()

        stdout = bstdout.decode("utf-8")
        stderr = bstderr.decode("utf-8")

    if "error" in stderr:
        assert False, stderr

    # pylint: disable=too-many-return-statements
    def _filter(line: str) -> bool:
        """
        I just want to remove the noise, so I can see what changes
        """

        if "import" in line:
            return False

        if "os.environ" not in line:
            return False

        if "__LM" in line:
            return False

        if "__LMFILES__" in line:
            return False

        if "_LMFILES_" in line:
            return False

        if "_ModuleTable" in line:
            return False

        return True

    def _split_line(line: str) -> Tuple[str, str]:

        # format:
        # os.environ["key"] = "value:value"

        _line = line.split("=")

        value = _line[-1]
        value = value.strip()

        if value[-1] == ";":
            value = value[:-1]

        value = value[1:-1]

        key = _line[0]
        key = key.strip()
        key = key.replace("os.environ", "")
        key = key[2:-2]

        return key, value

    # Filter some of the lines
    lines = stdout.split("\n")
    lines = [line for line in lines if _filter(line)]
    keyvalues = [_split_line(line) for line in lines]

    environment_update = dict(keyvalues)

    pythonpath = environment_update.get("PYTHONPATH", None)

    if pythonpath is not None:

        pythonpaths = pythonpath.split(":")

        for path in pythonpaths:

            if path in sys.path:
                continue

            sys.path.append(path)

    # update
    os.environ.update(environment_update)

    for key, value in environment_update.items():
        _logger.debug(f"{key} = {value}")

    return stderr


def purge() -> None:
    """Warning: This will break stuff"""
    raise NotImplementedError
    module("purge", "")


def load(module_name: str) -> None:
    """use `module load` to overload your environment"""
    module("load", module_name)


def use(path: str | Path) -> None:
    """Use path in MODULEPATH"""
    module("use", str(path))


def get_modules() -> dict[str, str]:
    """Return all active LMOD modules. Hidden modules are ignored."""

    stderr = module("list", "")

    assert stderr is not None

    lines = stderr.split("\n")

    # Filter to only lines with modules
    lines = [x for x in lines if ")" in x]
    lines = [x for x in lines if "Hidden Module" not in x]

    module_names = dict()
    for line in lines:

        line = line.strip()
        line = " ".join(line.split())  # replace multiple spaces with one
        line = line.replace(" (H)", "@(H)")
        line = line.replace(") ", "$$")
        line_ = line.split()

        for mod in line_:

            if "(H)" in mod:
                continue

            idx, name = mod.split("$$")
            module_names[idx] = name

    return module_names


def get_paths() -> List[str]:
    """Return all LMOD paths in use"""
    paths = os.environ.get("MODULEPATH", "")
    paths_ = paths.split(":")
    return paths_
