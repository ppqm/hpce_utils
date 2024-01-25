"""
Module for manging UGE submission and environments.

ARC – The architecture name of the node on which the job is running. The
name is compiled into the sge_execd binary.

SGE_ROOT – The root directory of the grid engine system as set for sge_execd
before startup, or the default /usr/SGE directory.

SGE_BINARY_PATH – The directory in which the grid engine system binaries are
installed.
SGE_CELL – The cell in which the job runs.

SGE_JOB_SPOOL_DIR – The directory used by sge_shepherd to store job-related
data while the job runs.

SGE_O_HOME – The path to the home directory of the job owner on the host from
which the job was submitted.

SGE_O_HOST – The host from which the job was submitted.

SGE_O_LOGNAME – The login name of the job owner on the host from which the job
was submitted.

SGE_O_MAIL – The content of the MAIL environment variable in the context of the
job submission command.

SGE_O_PATH – The content of the PATH environment variable in the context of the
job submission command.

SGE_O_SHELL – The content of the SHELL environment variable in the context of
the job submission command.

SGE_O_TZ – The content of the TZ environment variable in the context of the job
submission command.

SGE_O_WORKDIR – The working directory of the job submission command.

SGE_CKPT_ENV – The checkpointing environment under which a checkpointing job
runs. The checkpointing environment is selected with the qsub -ckpt command.

SGE_CKPT_DIR – The path ckpt_dir of the checkpoint interface. Set only for
checkpointing jobs. For more information, see the checkpoint(5) man page.

SGE_STDERR_PATH – The path name of the file to which the standard error stream
of the job is diverted. This file is commonly used for enhancing the output
with error messages from prolog, epilog, parallel environment start and stop
scripts, or checkpointing scripts.

SGE_STDOUT_PATH – The path name of the file to which the standard output stream
of the job is diverted. This file is commonly used for enhancing the output
with messages from prolog, epilog, parallel environment start and stop scripts,
or checkpointing scripts.

SGE_TASK_ID – The task identifier in the array job represented by this task.

ENVIRONMENT – Always set to BATCH. This variable indicates that the script is
run in batch mode.

HOME – The user's home directory path as taken from the passwd file.

HOSTNAME – The host name of the node on which the job is running.

JOB_ID – A unique identifier assigned by the sge_qmaster daemon when the job
was submitted. The job ID is a decimal integer from 1 through 9,999,999.

JOB_NAME – The job name, which is built from the file name provided with the
qsub command, a period, and the digits of the job ID. You can override this
default with qsub -N.

LOGNAME – The user's login name as taken from the passwd file.

NHOSTS – The number of hosts in use by a parallel job.

NQUEUES – The number of queues that are allocated for the job. This number is
always 1 for serial jobs.

NSLOTS – The number of queue slots in use by a parallel job.

PATH – A default shell search path of: /usr/local/bin:/usr/ucb:/bin:/usr/bin.

PE – The parallel environment under which the job runs. This variable is for
parallel jobs only.

PE_HOSTFILE – The path of a file that contains the definition of the virtual
parallel machine that is assigned to a parallel job by the grid engine system.
This variable is used for parallel jobs only. See the description of the
$pe_hostfile parameter in sge_pe for details on the format of this file.

QUEUE – The name of the queue in which the job is running.

REQUEST – The request name of the job. The name is either the job script file
name or is explicitly assigned to the job by the qsub -N command.

RESTARTED – Indicates whether a checkpointing job was restarted. If set to
value 1, the job was interrupted at least once. The job is therefore restarted.

SHELL – The user's login shell as taken from the passwd file.

TMPDIR – The absolute path to the job's temporary working directory.

TMP– The same as TMPDIR. This variable is provided for compatibility with NQS.

TZ – The time zone variable imported from sge_execd, if set.

USER – The user's login name as taken from the passwd file.

OMP_NUM_THREADS - Number of OpenMP threads

OPENBLAS_NUM_THREADS - Number of BLAS threads

VECLIB_MAXIMUM_THREADS - Number Vectorlib threads

MKL_NUM_THREADS - Number of Intel MathKernel threads

NUMEXPR_NUM_THREADS - Fast numerical expression evaluator for NumPy
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from hpce_utils.managers.uge import constants, status, submitting
from hpce_utils.shell import which

_logger = logging.getLogger(__name__)


def has_uge() -> bool:
    """Check if cluster has UGE setup"""

    exe = which(constants.command_submit)

    if exe is not None:
        return True

    return False


def is_uge() -> bool:
    """Check if module is called from a UGE queue enviroment"""

    name = os.getenv("SGE_TASK_ID")

    if name is None:
        return False

    return True


def get_env() -> Dict[str, Optional[str]]:
    """
    Get all UGE related environmental variables.

    important keywords are
        NSLOTS - Number of cores in current job
        TMPDIR - Node specific tmpdir

    """

    properties = {}

    for key in constants.UGE_KEYWORDS:
        properties[key] = os.getenv(key)

    return properties


def get_tmpdir() -> Path:
    """From UGE environment, get scratch directory"""

    tmpdir = os.getenv("TMPDIR")
    assert tmpdir is not None
    path = Path(tmpdir)
    return path


def get_config() -> Dict[str, Any]:
    """Get UGE configuration

    - Number of cores avaiable on node
    - Scratch directory on node
    - Hostname of current node
    """

    n_cores = os.getenv("NSLOTS")
    scr = os.getenv("TMPDIR")
    hostname = os.getenv("HOSTNAME")

    assert n_cores is not None
    assert scr is not None
    assert hostname is not None

    config = {
        "n_cores": int(n_cores),
        "scr": scr,
        "hostname": scr,
    }

    return config


def get_cores() -> int:
    """Get avaiable cores in current environment"""

    key = "NSLOTS"
    n_cores = os.getenv(key)
    assert n_cores is not None

    n_cores_ = int(n_cores)

    return n_cores_


def is_interactive():
    """Check if job is run via interactive shell (e.i. qrsh), or submission"""

    uge_type = os.getenv("REQUEST", None)

    # Not UGE
    if not uge_type:
        return False

    # if request is qrlogin, then qrsh was used
    if uge_type == "QRLOGIN":
        return True

    return False
