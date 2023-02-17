import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="snakestage",
    version="0.1",
    author="Maarten Kooyman",
    author_email="maarten@oyat.nl",
    description="Using a snakemake pipeline taking staging file into account (based on GFAL2 and SLURM)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/maarten-k/snakestage",
    packages=setuptools.find_packages(),
        scripts=[
        "bin/snakestage.py"
    ],
     install_requires=[
        "gfal2_python",
        "snakemake",
    ],
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Linux",
    ),
)
