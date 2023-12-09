import multiprocessing
import os
import shutil
from pathlib import Path
from typing import Optional, Union

from hpc_env import uge


def which(cmd: Union[str, Path]) -> Optional[Path]:
    """Check if command exists in enviroment"""
    path_ = shutil.which(cmd)

    if path_ is None:
        return None

    path = Path(path_)

    return path


def get_available_cores() -> int:

    n_cores: Optional[int]

    if is_uge():
        n_cores = uge.get_cores()

    else:
        n_cores = get_threads()

    if n_cores is None:
        n_cores = multiprocessing.cpu_count()

    assert n_cores is not None

    return n_cores


def get_threads() -> Optional[int]:
    """Get number of threads from environmental variables"""

    n_cores = os.environ.get("OMP_NUM_THREADS", None)
    n_cores = n_cores or os.environ.get("MKL_NUM_THREADS", None)
    n_cores = n_cores or os.environ.get("OPENBLAS_NUM_THREADS", None)

    if n_cores is None:
        return None

    assert n_cores is not None  # ffs mypy, you already know it is not none
    return int(n_cores)


def set_threads(n_cores: int) -> None:
    """

    Wrapper for setting environmental variables related to threads and procs.

    export OMP_NUM_THREADS=4
    export OPENBLAS_NUM_THREADS=4
    export VECLIB_MAXIMUM_THREADS=4
    export MKL_NUM_THREADS=4
    export NUMEXPR_NUM_THREADS=4

    :param n_cores: Number of threads to be used internally in compiled
    programs and libs.

    """

    n_cores_ = str(n_cores)

    os.environ["OMP_NUM_THREADS"] = n_cores_
    os.environ["OPENBLAS_NUM_THREADS"] = n_cores_
    os.environ["MKL_NUM_THREADS"] = n_cores_
    os.environ["VECLIB_MAXIMUM_THREADS"] = n_cores_
    os.environ["NUMEXPR_NUM_THREADS"] = n_cores_


def get_shm_path() -> Optional[Path]:
    """
    Get shared memory path for current node.

    /run/shm which was previously is temporary world-writable shared-memory.

    It is strictly intended as storage for programs using the POSIX Shared
    Memory API.
    """

    mem_path = "/run/shm/"
    mem_path_old = "/dev/shm/"

    path = Path(mem_path)

    if not path.is_dir():
        path = Path(mem_path_old)

    if not path.is_dir():
        return None

    return path


def is_uge() -> bool:
    """Check if module is called from a UGE queue enviroment"""

    name = os.getenv("SGE_TASK_ID")

    if name is None:
        return False

    return True


# pylint: disable=no-else-return
def is_notebook() -> bool:
    """
    Check if module is called from a notebook enviroment
    """
    try:
        shell = get_ipython().__class__.__name__  # type: ignore
        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        elif shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter
