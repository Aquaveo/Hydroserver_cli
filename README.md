# HydroServer Data Uploader

This Python script provides a command-line interface for uploading stations, observations, and datastreams to a HydroServer. The script uses the `httpx` library for making HTTP requests and `pandas` for handling CSV data.

## Prerequisites

- Python 3.x
- Required Python packages: `httpx`, `pandas`

## Installation

1. **Clone the repository:**

```github
git clone <repository_url>
cd hydroserver-uploader
```

2. **Install the required packages:**

```github
pip install -r requirements.txt
```

## Usage

### Upload Stations

To upload stations to a HydroServer, use the following command:

```github
python hydroserver_uploader.py upload-things <username> <password> <hydroserver_url> <file_path>

```

- `<username>`: HydroServer username
- `<password>`: HydroServer password
- `<hydroserver_url>`: HydroServer URL
- `<file_path>`: Path to the file containing station information (CSV format)

### Upload Observations

To upload observations to a HydroServer, use the following command:

```github
python hydroserver_uploader.py upload-observations <username> <password> <hydroserver_url> <file_path> <date_column> <value_column> <datastream_id>
```

- `<username>`: HydroServer username
- `<password>`: HydroServer password
- `<hydroserver_url>`: HydroServer URL
- `<file_path>`: Path to the file containing observation data (CSV format)
- `<date_column>`: Column name for date values
- `<value_column>`: Column name for observation values
- `<datastream_id>`: ID of the datastream to associate the observations with

### Delete All Things

To delete all stations from a HydroServer, use the following command:

```github
python hydroserver_uploader.py delete-all-things <username> <password> <hydroserver_url>
```

- `<username>`: HydroServer username
- `<password>`: HydroServer password
- `<hydroserver_url>`: HydroServer URL

## Notes

- Ensure that the required Python packages are installed (`httpx` and `pandas`) before running the script.
- Make sure to replace `<repository_url>` with the actual URL if you are cloning the repository.

Feel free to modify the script according to your specific needs and requirements.
