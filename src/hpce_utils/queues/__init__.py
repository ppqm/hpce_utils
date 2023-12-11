
from hpc_env.queues import uge, slurm

def get_cores()->int | None:

    n_cores = None

    if uge.is_uge():
        n_cores = uge.get_cores()

    elif slurm.is_slurm():
        n_cores = slurm.get_cores()

    return n_cores


