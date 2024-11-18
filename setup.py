from setuptools import setup  # type: ignore


def main():

    setup(
        name="hpce_utils",
        version="0",
        python_requires=">=3.6",
        install_requires=[],
        packages=["hpce_utils"],
        package_dir={"": "src"},
    )

    return


main()
