# Hephaestus

**Hephaestus** is a repository designed to house various ETL projects focuses on being structured to facilitate the development, testing and deployment modular, resuable and maintained *Data Engineering Pipelines* to support the future *Data Analysis* projects of the (upcoming) repositorio [Athena]().

## Repository Structure
The repository is organized into the following main directories:

- **core/**: This directory contains core utilities and libraries that are used across multiple ETL projects.
  - **utils/**: Helper functions and general utilities are stored here, with each helper module named using the pattern `{name}_helper.py`.
  - **lib/**: Quality assurance functions are stored here to ensure the reliability and consistency of the data, with each module named using the pattern `{name}_qa.py`.

- **pipelines/**: This directory contains all individual ETL projects. Each project has its own subdirectory, which includes the necessary scripts, configurations, and data transformation logic. Each subfolder includes:
  - **docs/**: Documentation specific to the pipeline project.
  - **notebooks/**: Jupyter notebooks used for exploration, prototyping, or documentation purposes.
  - **research/**: Research notes, data sources, and other relevant materials.
  - **config/**: Configuration files required for the ETL pipeline, such as `.yaml`, `.json`, or `.env` files that define environment variables, database connections, API keys, and other settings.

- **src/**: This directory contains the source code for the core logic of the ETL processes. It may include modules that are specific to certain pipelines but are general enough to be reused.

- **tests/**: This directory is for all unit tests and integration tests associated with the ETL projects. Testing is crucial to ensure that each part of the pipeline works correctly and as expected.

- **docs/**: High-level documentation for the entire repository, including an overview of the ETL pipelines.