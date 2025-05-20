from setuptools import setup, find_packages

setup(
    name="hephaestus_pframe-pframe",
    version="0.1.0",
    packages=find_packages(where="hephaestus_pframe"),
    package_dir={"": "hephaestus_pframe"},
    install_requires=[
        "numpy",
        "psutil",
        "python-dotenv",
        "PyYAML",
        "requests",
        "SQLAlchemy",
        "tqdm"
    ],
    include_package_data=True,
)
