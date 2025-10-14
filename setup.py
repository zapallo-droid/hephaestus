from setuptools import setup, find_packages

setup(
    name="hephaestus_forge",
    version="0.1.0",
    description="ETL personal tracebility wrapper",
    packages=find_packages(include=["hephaestus_forge", "hephaestus_forge.*"]),
    include_package_data=True,
    python_requires=">=3.10",
    install_requires=[
        "SQLAlchemy>=2.0",
        "python-dotenv>=1.0",
        "PyYAML>=6.0",
        "typing_extensions>=4.9",
        "greenlet>=3.0",
        "psycopg2-binary>=2.9",
    ],
    extras_require={
        "dev": [
            "ipykernel",
            "ipython",
            "jupyter_client",
            "jupyter_core",
            "debugpy",
            "pyzmq",
            "Pygments",
            "nest-asyncio",
        ],
    },
)
