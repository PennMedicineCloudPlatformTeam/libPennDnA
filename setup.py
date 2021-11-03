from setuptools import find_packages, setup
import unittest

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='DACPYLib',
    packages=find_packages(),
    version='0.0.2',
    author='Kevin Long',
    author_email='kevin.long@pennhealth.upenn.edu',
    description='DAC Toolset package',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url={"https://dev.azure.com/UPHS-DAC/AnalyticsPipeline/_git/DACPYLib"
        "Bug Tracker": "https://dev.azure.com/UPHS-DAC/AnalyticsPipeline/_git/DACPYLib",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    python_requires=">=3.6"
)
