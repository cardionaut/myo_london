# Data converter for RedCap import and export (FlamBeR) <!-- omit in toc -->

## Table of contents <!-- omit in toc -->

- [Installation](#installation)
- [Functionalities](#functionalities)
- [Configuration](#configuration)
  - [Export -\> config.yaml](#export---configyaml)
  - [Import -\> config\_upload.yaml](#import---config_uploadyaml)
- [Run](#run)

## Installation

```bash
    python3 -m venv env
    source env/bin/activate
    pip install poetry
    poetry install
```

## Functionalities

Use **main.py** to convert a file exported from RedCap FlamBeR to a cleaned file ready to use with ML pipelines (i.e. one row per patient).\
Use **redcap_upload.py** to prepare data collected by students (strain, function, LGE, etc.) for upload/import to RedCap FlamBeR.
  
## Configuration

### Export -> config.yaml

Specify file_path, which columns to drop and the MACE types.

### Import -> config_upload.yaml

Specify path to RedCap template file and path to available files from students, use null for unavailable files so they are skipped.

## Run

After the config files are set up properly, you can run the pipeline using:

```bash
python3 main.py
```

or

```bash
python3 redcap_upload.py
```
