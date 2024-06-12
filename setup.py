import unittest
import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='libPennDnA',
    packages=setuptools.find_packages(where="src"),
    version='0.0.9',
    author='Kevin Long',
    author_email='kevin.long@pennhealth.upenn.edu',
    description='PennDnA Toolset package',
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    python_requires=">=3.6",
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    test_suite="tests",
)
