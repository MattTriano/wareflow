from setuptools import setup, find_packages

with open("requirements.txt", "r") as reqs_file:
	requirements = reqs_file.readlines()

setup(
	author="Matt Triano",
    name="wareflow",
	description="Tools for managing a personal data warehouse",
    version="0.1.0",
	packages=find_packages(),
	include_package_data=True,
    install_requirements=requirements
)
