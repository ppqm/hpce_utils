UGE_TASK_ID = "$SGE_TASK_ID"

qsub = "qsub"
command_submit = f"{qsub} {{filename}}"
command_submit_wait = f"{qsub} -sync y {{filename}}"
command_run_shell = (
    "qrsh -N '{name}' -cwd -V -verbose -l "
    "mem_free={mem}G,h_rt={hours}:00:00 -pe smp {cores} 'bash {filename}'"
)
