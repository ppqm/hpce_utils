import logging
from pathlib import Path

from jinja2 import Template

from hpce_utils.files import generate_name
from hpce_utils.managers.uge import constants
from hpce_utils.shell import execute

DEFAULT_LOG_DIR = "./ugelogs/"
TEMPLATE_TASKARRAY = Path(__file__).parent / "templates" / "submit-task-array.jinja"
logger = logging.getLogger(__name__)


def generate_command(sync=False, export=False)->str:
    """ Generate UGE/SGE submit command with approriate flags

    export:
       Available for qsub, qsh, qrsh, qlogin and qalter.
       Specifies that all environment variables active within the qsub utility
       be exported to the context  of  the job.

    sync:
       Available for qsub.
       wait for the job to complete before exiting. If the job completes
       successfully, qsub's exit code will be that of the completed job.

    """

    qrsh = constants.command_submit

    cmd = [qrsh]

    if sync:
        cmd.append(constants.flag_sync)

    if export:
        cmd.append(constants.flag_environment_export)

    return " ".join(cmd)



# pylint: disable=too-many-arguments,too-many-locals,dangerous-default-value
def generate_taskarray_script(
    cmd: str,
    cores: int = 1,
    cwd: Path | None = None,
    environ: dict[str, str] = {},
    hours: int = 7,
    mins: int | None = None,
    log_dir: Path | str | None = DEFAULT_LOG_DIR,
    mem: int = 4,
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

    if not isinstance(cores, int) and cores >= 1:
        raise ValueError(
            "Cannot submit with invalid cores set. Needs to be a integer greater than 0."
        )

    kwargs = locals()

    if not log_dir.exists():
        log_dir.mkdir(parents=True)

    if log_dir.is_dir():
        log_dir = str(log_dir.resolve() / "_")[:-1]  # Added a trailing slash

    kwargs["log_dir"] = log_dir

    with open(TEMPLATE_TASKARRAY) as file_:
        template = Template(file_.read())

    script = template.render(**kwargs)

    return script


# pylint: disable=dangerous-default-value
def submit_script(
    submit_script: str,
    scr: str | Path | None = None,
    filename: str | None = None,
    cmd: str = constants.command_submit,
    cmd_options: dict[str, str] = {},
    dry: bool = False,
) -> tuple[str | None, Path | None]:
    """Submit script and return UGE Job ID

    return:
        job_id
        script path
    """

    if filename is None:
        filename = f"tmp_uge.{generate_name()}.sh"

    if scr is None:
        scr = "./"

    scr = Path(scr)
    scr.mkdir(parents=True, exist_ok=True)

    with open(scr / filename, "w") as f:
        f.write(submit_script)

    logger.debug(f"Writing {filename} for UGE on {scr}")

    # TODO Needs some re-checks
    cmd = f"{cmd} {{filename}}"
    cmd = cmd.format(filename=filename, **cmd_options)
    logger.debug(cmd)
    logger.debug(scr)

    if dry:
        logger.info("Dry submission of qsub command")
        logger.info(f"cmd={cmd}")
        logger.info(f"scr={scr}")
        return None, scr / filename

    stdout, stderr = execute(cmd, cwd=scr)

    if stderr:
        for line in stderr.split("\n"):
            logger.error(line)
        return None, scr / filename

    if not stdout:
        logger.error("Unable to fetch qsub job id from stdout")
        return None, scr / filename

    # Successful submission
    # find id
    logger.info(stdout.strip().rstrip())

    # Your job JOB_ID ("JOB_NAME") has been submitted
    uge_id = stdout.split()[2]
    uge_id = uge_id.split(".")
    uge_id = uge_id[0]

    return uge_id, scr / filename


def delete_job(job_id: str | int) -> None:

    cmd = f"qdel {job_id}"
    logger.debug(cmd)

    stdout, stderr = execute(cmd)
    stdout = stdout.strip()
    stderr = stderr.strip()

    for line in stderr.split("\n"):
        logger.error(line)

    for line in stdout.split("\n"):
        logger.error(line)


def read_logfiles(
    log_path: Path, job_id: str, ignore_stdout=True
) -> tuple[dict[Path, list[str]], dict[Path, list[str]]]:
    """Read logfiles produced by UGE task array. Ignore empty log files"""
    logger.debug(f"Looking for finished log files in {log_path}")
    stderr_log_filenames = log_path.glob(f"*.e{job_id}*")
    stderr = dict()
    for filename in stderr_log_filenames:
        if filename.stat().st_size == 0:
            continue
        stderr[filename] = parse_logfile(filename)

    if ignore_stdout:
        return dict(), stderr

    stdout_log_filenames = log_path.glob(f"*.o{job_id}*")
    stdout = dict()
    for filename in stdout_log_filenames:
        if filename.stat().st_size == 0:
            continue
        stdout[filename] = parse_logfile(filename)

    return stdout, stderr


def parse_logfile(filename: Path):
    # TODO Maybe find exceptions and raise them?
    with open(filename, "r") as f:
        lines = f.readlines()
    return lines


# def uge_log_error(filename: Path, name: str = "uge", logger=_logger) -> None:

#     if not filename.exists():
#         logger.error(f"could not read {filename}")
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
