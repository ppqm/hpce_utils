import multiprocessing
import os
import shutil
from pathlib import Path
from typing import Optional, Union

from hpce_utils import managers
from hpce_utils.env import lmod

ENVIRON_CORES = [
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "NUMEXPR_NUM_THREADS",
]


def get_available_cores() -> int:

    n_cores: Optional[int]

    n_cores = managers.get_cores()

    if n_cores is not None:
        return n_cores

    n_cores = get_threads()

    if n_cores is not None:
        return n_cores

    n_cores = multiprocessing.cpu_count()

    if n_cores is None:
        raise ValueError("Could not find avail. cores")

    return n_cores


def get_threads() -> Optional[int]:
    """Get number of threads from environmental variables"""

    n_cores = None

    for name in ENVIRON_CORES:
        n_cores = os.environ.get(name, None)

        if n_cores:
            break

    if n_cores is None:
        return None

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

    for name in ENVIRON_CORES:
        os.environ[name] = n_cores_


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
    if path.is_dir():
        return path

    path = Path(mem_path_old)
    if path.is_dir():
        return path

    return None


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
