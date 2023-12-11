# pylint: disable=too-many-arguments,too-many-locals,dangerous-default-value
def get_submit_script(
    cmd: str,
    cores: int = 4,
    cwd: Optional[Path] = None,
    environ: Dict[str, str] = {},
    hours: int = 12,
    log_dir: Optional[Path] = None,
    mem: int = 4,
    module_load: List[str] = [],
    module_use: List[str] = [],
    name: str = "UGEJob",
    task_concurrent: int = 100,
    task_start: int = 1,
    task_step: int = 1,
    task_stop: Optional[int] = None,
) -> str:
    # pylint: disable=unused-argument

    kwargs = locals()

    if log_dir is not None:
        # Ensure log_dir has trailing_path
        log_dir_ = directory_add_trail(str(log_dir.resolve()))
        kwargs["log_dir"] = log_dir_

    template = Template(SCRIPT_TEMPLATE_JINJA2)
    msg = template.render(**kwargs)

    return msg


# pylint: disable=dangerous-default-value
def submit(
    submit_script: str,
    scr: Union[str, Path] = HERE,
    filename: str = "tmp_uge.sh",
    cmd: str = QSUB,
    cmd_options: Dict[str, str] = {},
    dry: bool = False,
) -> Optional[str]:
    """submit script and return UGE ID"""

    scr = Path(scr)
    scr.mkdir(parents=True, exist_ok=True)

    with open(scr / filename, "w") as f:
        f.write(submit_script)

    logger.debug(f"Writing {filename} for UGE on {scr}")

    if dry:
        return None

    # TODO Should be random name to avoid raise-condition
    cmd = cmd.format(filename=filename, **cmd_options)
    logger.debug(cmd)
    logger.debug(scr)

    stdout, stderr = execute(cmd, cwd=scr)

    if stderr:
        for line in stderr.split("\n"):
            logger.error(line)
        return None

    # Successful submission
    # find id
    logger.info(stdout.strip().rstrip())

    # Your job JOB_ID ("JOB_NAME") has been submitted
    uge_id = stdout.split()[2]
    uge_id = uge_id.split(".")
    uge_id = uge_id[0]

    return uge_id


def delete(job_id: Union[str, int]) -> None:

    cmd = f"qdel {job_id}"
    logger.debug(cmd)

    stdout, stderr = execute(cmd)
    stdout = stdout.strip()
    stderr = stderr.strip()

    for line in stderr.split("\n"):
        logger.error(line)

    for line in stdout.split("\n"):
        logger.error(line)


def get_status(job_id: Union[str, int]) -> Optional[Dict[str, str]]:
    """
    Get status of SGE Job
    job_state
        q - queued
        qw - queued wait ?
        r - running
        dr - deleting
        dx - deleted
    job_name
    """

    cmd = f"qstat -j {job_id}"
    # TODO Check syntax for task-array

    stdout, stderr = execute(cmd)

    stderr = stderr.replace("\n", "")
    stderr = stderr.strip().rstrip()

    if stderr:
        logger.debug(stderr)
        return None

    lines = stdout.split("\n")

    status_ = dict()

    for line in lines:

        line_ = line.split(":")
        if len(line_) < 2:
            continue

        # TODO Is probably task_array related and needs are more general fix
        # job_state             1:    r
        key = line_[0]
        key = key.replace("    1", "")
        key = key.strip()

        content = line_[1].strip()

        status_[key] = content

    return status_


def execute(
    cmd: str, cwd: Optional[Path] = None, shell: bool = True, timeout: Optional[int] = None
) -> Tuple[str, str]:
    """Execute command in directory, and return stdout and stderr
    :param cmd: The shell command
    :param cwd: Change directory to work directory
    :param shell: Use shell or not in subprocess
    :param timeout: Stop the process at timeout (seconds)
    :returns: stdout and stderr as string
    """

    with subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        shell=shell,
        cwd=cwd,
    ) as process:

        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            logger.error("Process timed out")
            stdout = ""
            stderr = ""

    return stdout, stderr


def wait_for_jobs(jobs: List[str], respiratory: int = 60):
    """Monitor UGE jobs and yield when jobs are done"""

    logger.info(f"Submitted {len(jobs)} job(s) to UGE cluster")

    start_time = time.time()

    while len(jobs):

        logger.info(f"... and breathe for {respiratory} sec, still running {len(jobs)} job(s)")
        time.sleep(respiratory)

        for job_id in jobs:

            job_is_done = _uge_is_job_done(job_id)

            if job_is_done:
                yield job_id
                jobs.remove(job_id)

    end_time = time.time()
    diff_time = end_time - start_time

    logger.info(f"All jobs finished and took {diff_time/60/60:.2f}h")


def _uge_is_job_done(job_id: str) -> bool:

    still_waiting_states = ["q", "r", "qw", "dr"]

    status = get_status(job_id)

    if status is None:
        return True

    state = status.get("job_state", "qw")
    logger.debug(f"uge {job_id} is {state}")

    if state not in still_waiting_states:
        return True

    return False


def uge_log_error(filename: Path, name: str = "uge") -> None:

    if not filename.exists():
        logger.error(f"could not read {filename}")
        return

    with open(filename, "r") as f:
        for line in f:
            line = line.strip()
            logger.error(f"{name} - {line}")
