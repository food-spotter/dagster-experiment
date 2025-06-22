from setuptools import find_packages, setup

setup(
    name="new_idea",
    packages=find_packages(exclude=["new_idea_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
