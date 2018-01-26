import re
from setuptools import setup

version = "0.0.0"

with open("README.md", "rb") as f:
  long_descr = f.read().decode("utf-8")
    
setup(
  name="scheduler_scripts",
  packages=["scheduler_scripts"],
  entry_points={"console_scripts":['queuewait=scheduler_scripts.queuewait:main','sacctall=scheduler_scripts.sacctall:main']},
  version=version,
  description="A number of scripts found useful when analyzing slurm jobs",
  long_description=long_descr,
  author="Chris Geroux",
  author_email="chris.geroux@ace-net.ca",
  url="",
  include_package_data=True
  )