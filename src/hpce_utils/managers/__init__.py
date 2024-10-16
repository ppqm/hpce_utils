from typing import Optional

from hpce_utils.managers import slurm, uge


def get_cores() -> Optional[int]:

    n_cores = None

    if uge.is_uge():
        n_cores = uge.get_cores()

    # elif slurm.is_slurm():
    #     n_cores = slurm.get_cores()

    return n_cores
