from setuptools import setup

setup(
    name="pcs_scraper",
    version="0.1",
    description="A package to scrape pro cycling stats",
    author="Ruairi O'Driscoll",
    license="MIT",
    install_requires=[
        "argparse",
        "beautifulsoup4",
        "lxml", 
        "tqdm", 
        "pandas",
        "pyarrow"
    ],
)