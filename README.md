# Hephaestus

**DISCLAIMER:** This is a personal portfolio project by someone who is not a Data Engineer. I'm a Data Analyst / Data Scientist exploring how to build, structure, and monitor real ETL pipelines. This is a playground—but one I take seriously.
**CURRENT STATUS:** *TESTING*

## ⚙️ What is Hephaestus?

Hephaestus is a modular and extensible ETL framework designed to help me:

- Build structured pipelines for multiple data domains.

- Track and monitor what happens inside them.

- Collect useful data and metadata along the way for later use in analysis, visualization, or machine learning projects.

It serves as a **backbone for my portfolio**, providing a clean, repeatable way to extract, transform, and load data, while logging everything that happens in the process.

It connects:

- **Cosmos** → the unstructured file system (e.g., local folders or S3).

- **Elysium** → the structured PostgreSQL database that stores both data and pipeline metadata (e.g., jobs, tasks, logs).

## 🔧 Core Design Principles

- **Modular & Reusable:** The same core logic (Job, Task, BucketHandler) works across different pipelines.

- **Decoupled:** Code, configs, logs, and DB schemas are separated.

- **Observability-first:** All jobs and tasks track their own runtime, performance, and exceptions using the operations schema in Elysium.

- **Generic, but pragmatic:** Built to be useful, not perfect. It’s designed for me, but I write it as if others might read it. It is, in the end, just yet another Portfolio Project.

## 🧱 Architecture Overview

```
hephaestus/
├── hephaestus/              # Core package
│   ├── core/                # Base ETL logic (jobs, tasks, handlers)
│   │   ├── utils/               # Utilities and helpers
│   │   ├── model/               # Data models for operations and Elysium
│   └── tests/               # Unit tests
│
├── examples/                # Pipeline examples (one per domain or source)
├── docs/                    # General documentation
├── setup.py                 # Package installer
├── requirements.txt         # Dependencies
└── README.md                # You're here
```

## 🧠 Cosmos & Elysium

- **Cosmos** is the raw data layer, used to persist .json.gz or similar files after extraction or transformation.

- **Elysium** is the structured database (PostgreSQL) where:

  - Final outputs are stored.

  - Operational logs and metadata are stored in the operations schema.

Each project can define its own data models in its own schema (tacticum, forum, etc.), while Hephaestus uses operations and optionally common for shared structures.

## 🛠️ How to Build a Pipeline

Each pipeline should:

1. Inherit from `Job` and `Task` in the core framework.

2. Each project shall contain its own pipelines TaskHandlers as shown in the examples.

3. Write raw outputs to Cosmos using `BucketHandler`.

4. Persist structured data in Elysium, under its own schema.

```
from hephaestus.jobs import Job
from hephaestus.tasks import Task

class MyPipeline(Job):
    ...
```

To initialize the database with the operational schema:
```
python -m hephaestus.db.init_schema
```

## 🚀 Getting Started
```
git clone https://github.com/zapallo-droid/hephaestus.git
pip install -e ./hephaestus
```
Then:

- Use it inside the data project (e.g., Tacticum).

- Write pipelines using the job/task framework.

- Collect structured and unstructured outputs as we go.

## 💬 Contributions & Community

This is not an open-source package in the strict sense. I’m building it for myself, but if you have thoughts or ideas or spot something useful or broken, feel free to open an issue or send a message.

Contributions are welcome as suggestions, but I'm not aiming for full community maintenance. Again, this is just yet another portfolio project.

## 📜 License

MIT License -> use at your own risk and adapt as you like.

