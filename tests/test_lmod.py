import os
import sys

from hpc_env import lmod

# TODO Check if

# TODO Create temp module in assets

def test_use() -> None:

    module_path = "GET RESOURCES PATH" # TODO
    lmod.use(module_path)
    assert os.environ.get("MODULEPATH") is not None
    assert module_path in os.environ.get("MODULEPATH", "")


def test_use_and_load() -> None:
    lmod.use("")
    lmod.load("")
    assert hpc_env.which("") is not None
