from setuptools import find_packages, setup

setup(
    name="fantasy_nba",
    packages=find_packages(exclude=["fantasy_nba_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
