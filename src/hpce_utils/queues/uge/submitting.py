import logging
from pathlib import Path

from jinja2 import Template

from hpce_utils.queues.uge import constants
from hpce_utils.shell import execute

DEFAULT_LOG_DIR = "./ugelogs/"
TEMPLATE_TASKARRAY = Path(__file__).parent / "templates" / "submit-task-array.jinja"
logger = logging.getLogger(__name__)


# pylint: disable=too-many-arguments,too-many-locals,dangerous-default-value
def generate_taskarray_script(
    cmd: str,
    cores: int = 1,
    cwd: Path | None = None,
    environ: dict[str, str] = {},
    hours: int = 7,
    log_dir: Path | str | None = DEFAULT_LOG_DIR,
    mem: int = 4,
    module_load: list[str] = [],
    module_use: list[str] = [],
    name: str = "UGEJob",
    task_concurrent: int = 100,
    task_start: int = 1,
    task_step: int = 1,
    task_stop: int | None = None,
) -> str:
    """
    Remember:
      - To set core restrictive env variables
    """

    kwargs = locals()

    with open(TEMPLATE_TASKARRAY) as file_:
        template = Template(file_.read())

    script = template.render(**kwargs)

    return script


# pylint: disable=dangerous-default-value
def submit_job(
    submit_script: str,
    scr: str | Path | None = None,
    filename: str = "tmp_uge.sh",
    cmd: str = constants.command_submit,
    cmd_options: dict[str, str] = {},
    dry: bool = False,
) -> str | None:
    """Submit script and return UGE Job ID"""

    if scr is None:
        scr = "./"

    scr = Path(scr)
    scr.mkdir(parents=True, exist_ok=True)

    with open(scr / filename, "w") as f:
        f.write(submit_script)

    logger.debug(f"Writing {filename} for UGE on {scr}")

    # TODO Should be random name to avoid raise-condition
    cmd = cmd.format(filename=filename, **cmd_options)
    logger.debug(cmd)
    logger.debug(scr)

    if dry:
        logger.info("Dry submission of qsub command")
        logger.info(f"cmd={cmd}")
        logger.info(f"scr={scr}")
        return None

    stdout, stderr = execute(cmd, cwd=scr)

    if stderr:
        for line in stderr.split("\n"):
            logger.error(line)
        return None

    if not stdout:
        logger.error("Unable to fetch qsub job id from stdout")
        return None

    # Successful submission
    # find id
    logger.info(stdout.strip().rstrip())

    # Your job JOB_ID ("JOB_NAME") has been submitted
    uge_id = stdout.split()[2]
    uge_id = uge_id.split(".")
    uge_id = uge_id[0]

    return uge_id


# def delete_job(job_id: Union[str, int]) -> None:

#     cmd = f"qdel {job_id}"
#     logger.debug(cmd)

#     stdout, stderr = execute(cmd)
#     stdout = stdout.strip()
#     stderr = stderr.strip()

#     for line in stderr.split("\n"):
#         logger.error(line)

#     for line in stdout.split("\n"):
#         logger.error(line)


def parse_logfiles(log_path: Path, job_id: str, ignore_stdout=True) -> tuple[list[str], list[str]]:
    logger.debug(f"Looking for finished log files in {log_path}")
    stderr_log_filenames = list(log_path.glob(f"*.e{job_id}*"))
    print(stderr_log_filenames)
    stderr = []
    for filename in stderr_log_filenames:
        stderr += parse_logfile(filename)

    if ignore_stdout:
        return [], stderr

    stdout_log_filenames = list(log_path.glob(f"*.o{job_id}*"))
    print(stdout_log_filenames)
    stdout = []
    for filename in stdout_log_filenames:
        stdout += parse_logfile(filename)

    return stdout, stderr


def parse_logfile(filename: Path):
    # TODO Maybe find exceptions and raise them?
    with open(filename, "r") as f:
        lines = f.readlines()
    return lines


# def uge_log_error(filename: Path, name: str = "uge") -> None:

#     if not filename.exists():
#         _logger.error(f"could not read {filename}")
#         return

#     with open(filename, "r") as f:
#         for line in f:
#             line = line.strip().rstrip()
#             # ignore empty lines
#             if not line:
#                 continue
#             _logger.error(f"{name} - {line}")


# def read_logfiles(log_path, job_id):
#     """Check for errors in log files"""
#     log_filenames = log_path.glob(f"*.e{job_id}*")
#     for filename in log_filenames:
#         uge_log_error(filename, name=str(job_id))
