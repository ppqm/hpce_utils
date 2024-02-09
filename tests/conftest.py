import shutil
import tempfile
from pathlib import Path
from typing import Generator

import pytest

RESOURCES = Path("tests/resources/")


@pytest.fixture(scope="module")
def home_tmp_path() -> Generator[Path, None, None]:
    """Make a temporary directory in home

    Home is a globally mounted directory and therefore safe for UGE usage.
    """
    user_homedir = Path.home()
    random_name = next(tempfile._get_candidate_names())  # type: ignore[attr-defined]

    tmp_path = user_homedir / "tmp" / f"pytest_{random_name}"
    tmp_path.mkdir(parents=True, exist_ok=True)

    yield tmp_path

    # Force clean
    shutil.rmtree(tmp_path)
    assert not tmp_path.is_dir()
