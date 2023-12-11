
UGE_TASK_ID = "$SGE_TASK_ID"

QSUB = "qsub {filename}"
QSUB_WAIT = "qsub -sync y {filename}"
QRSH = (
    "qrsh -N '{name}' -cwd -V -verbose -l "
    "mem_free={mem}G,h_rt={hours}:00:00 -pe smp {cores} 'bash {filename}'"
)
