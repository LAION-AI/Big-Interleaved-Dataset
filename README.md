# Big Interleaved Dataset (BILD)
Big Interleaved Dataset is a LAION project that aims to create an open-source multimodal dataset like Deepmind M3W (MultiModal MassiveWeb dataset).

## Development Setup
1. Configure and install [Poetry](https://python-poetry.org/docs/#installation). We use this tool to manage our Python dependencies.
2. Setup your base Python 3.8.15 environment with tools such as [Miniconda](https://docs.conda.io/en/latest/miniconda.html).
2. Clone the project and run the following commands
```
cd big-interleaved-dataset/

# Setup the Python environment and install all associated dev packages
poetry install --with dev

# Activate the virutal environment
poetry shell

# Initialize pre-commit to setup formatting via Black, etc.
pre-commit install
```
3. We have configured our virutal environment and here are some helpful commands for development.
```
# Run a certain script with the virtual environment
poetry run python tests/adhoc_test.py

# To add a new package
poetry add numpy==1.24.1
poetry update
```

For more information around using Poetry, check out their [documentation](https://python-poetry.org/docs/).

### Notes
- `requirements.txt` is being auto-generated as a back-up way to configure the environment with `virtualenv`

## Communications
We discuss our ongoing project progress at #big-interleaved-dataset on [LAION Discord](https://discord.gg/kAyhUK3jyW).

Our weekly meeting time is usually Tuesday or Thursday at 8 PM CET. The meeting information will be provided in the channel.

## Design
Go to [Design](docs/design.md).
