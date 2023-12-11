import os
import sys
import pytest

from hpce_utils.env import lmod

if lmod.get_lmod_executable()
    pytest.skip("Could not find LMOD executable", allow_module_level=True)

def test_use() -> None:

    module_path = "GET RESOURCES PATH" # TODO
    lmod.use(module_path)
    assert os.environ.get("MODULEPATH") is not None
    assert module_path in os.environ.get("MODULEPATH", "")


def test_use_and_load() -> None:
    lmod.use("")
    lmod.load("")
    assert hpc_env.which("") is not None
