import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="DACPYLib Tools",
    version="0.0.1",
    author="Kevin Long",
    author_email="kevin.long@pennhealth.upenn.edu",
    description="DAC Toolset package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://dev.azure.com/UPHS-DAC/AnalyticsPipeline/_git/DACPYLib"
    project_urls={
        "Bug Tracker": "https://dev.azure.com/UPHS-DAC/AnalyticsPipeline/_git/DACPYLib",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
)