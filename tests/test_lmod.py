import os

import pytest
from conftest import RESOURCES

from hpce_utils.env import lmod

if not lmod.get_lmod_executable():
    pytest.skip("Could not find LMOD executable", allow_module_level=True)


MODULE_PATH = RESOURCES.absolute() / "lmod" / "modules"
MODULE_NAME = "test"


def test_use() -> None:

    lmod.use(MODULE_PATH)

    paths = os.environ.get("MODULEPATH")
    print(paths)

    assert os.environ.get("MODULEPATH") is not None, "No module path to be found"
    assert str(MODULE_PATH) in os.environ.get(
        "MODULEPATH", ""
    ), "Unable to find loaded module path"


def test_load() -> None:
    lmod.use(MODULE_PATH)

    print(os.environ.get("MODULEPATH"))

    # Check use
    assert str(MODULE_PATH) in os.environ.get(
        "MODULEPATH", ""
    ), "Unable to find loaded module path"

    lmod.load(MODULE_NAME)

    # Check env is set
    assert os.environ.get("TESTLMODMODULE") == "THIS IS A TEST"

    # Check path is updated
    binpaths = os.environ.get("PATH", "").split(":")
    print(binpaths)
    assert "/does/not/exist/bin" in binpaths
