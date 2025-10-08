# Landmapper Data Pipeline Library (lm-dpl)

A Python library for procuring and processing Landmapper geospatial data layers.

## Project Structure

```
lm-dpl/
├── lm_dpl/
│   ├── cli.py              # Command-line interface
│   ├── parcels/
│   │   └── processor.py    # Parcel data processing
│   ├── soil/
│   │   └── processor.py    # Soil data processing
│   ├── clients/            # Database and API clients
│   └── utils/              # Utility modules
├── tests/
│   └── test_cli.py         # CLI tests
├── pyproject.toml          # Package configuration
└── README.md               # You are here
```


## Install Development Environment

1. Clone the repository:
```bash
git clone https://github.com/Ecotrust/lm-dpl.git
cd lm-dpl
```

2. Setup your .env file with environment variables:
```
# LandMapper Data Pipeline Environment Configuration

# For reporiting errors via email (optional). Currently only Gmail is supported.
SENDER_EMAIL=<email>
GMAIL_APP_PASSWORD=<password>
RECIPIENT_EMAIL=<email>

# Logging configuration (optional - auto-generated if not set)
LOG_PATH=./logs/lm-dpl.log

# PostGIS service configuration
POSTGRES_DB=<database>
POSTGRES_USER=<username>
POSTGRES_PASSWORD=<password>
POSTGRES_HOST=<host>
POSTGRES_PORT=<port>
```

3. Install in editable mode with development dependencies:
```bash
pip install -e ".[dev]"
```

4. Run Docker container for PostGIS database (Optional):
```bash
cd docker && docker compose up -d
```


## Command-Line Interface

After installation, the `lm-dpl` command becomes available:

```bash
lm-dpl --help
```

### Process Parcel Data

Process parcel data for a specific state:

```bash
lm-dpl parcels <state>
```

`lm-dpl` supports both full state names and two letter abbreviations:

| Full Name | Abbreviation |
|-----------|-------------|
| `oregon`  | `OR`        |
| `washington` | `WA`    |


Use option `--layer` or `-l` to fetch individual layer(s). 

**Examples:**
```bash
# Process all parcel layers for Oregon (use full name or abbreviation)
lm-dpl parcels oregon

# Process only ODF Forest Protection Districts data layer
lm-dpl parcels --layer fpd oregon

```

### Process Soil Data

Process soil data for a specific state:

```bash
lm-dpl soil <state>
```

**Examples:**
```bash
# Process soil data for Oregon
lm-dpl soil oregon
```

