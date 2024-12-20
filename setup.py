import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="snakestage",
    version="0.1.10",
    author="Maarten Kooyman",
    author_email="maarten@oyat.nl",
    description="Using a snakemake pipeline taking staging file into account (based on dcache API and SLURM)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/maarten-k/snakestage",
    packages=["snakestage", "pmgridtools"],
    entry_points={"console_scripts": ["snakestage=snakestage.snakestage:main"]},
    install_requires=[
        "tqdm",
        "snakemake",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Linux",
    ],
)
