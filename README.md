
# HydroServer Data Upload Tool

This command-line tool allows you to upload data to a HydroServer, including stations, time series observations, and datastreams. You can also delete all stations if needed.

## Prerequisites

Before using this tool, ensure you have the following prerequisites installed:

- Python (3.7 or higher)
- Required Python packages (you can install them using `pip`):
  - `httpx`
  - `pandas`

## Usage

### Upload Stations

Upload station data to the HydroServer:

```bash
python upload_data.py upload-things <username> <password> <hydroserver_url> <file_path>
```

* `<username>`: Your HydroServer username.
* `<password>`: Your HydroServer password.
* `<hydroserver_url>`: The URL of the HydroServer.
* `<file_path>`: The path to the CSV file containing station data.


### Upload Datastreams

Upload datastreams to the HydroServer:

```
python upload_data.py delete-all-things <username> <password> <hydroserver_url> <file_path>

```

* `<username>`: Your HydroServer username.
* `<password>`: Your HydroServer password.
* `<hydroserver_url>`: The URL of the HydroServer.
* `<file_path>`: The path to the CSV file containing datastream information.


### Delete All Stations

Delete all stations from the HydroServer:

```
python upload_data.py delete-all-things <username> <password> <hydroserver_url> <file_path>
```

* `<username>`: Your HydroServer username.
* `<password>`: Your HydroServer password.
* `<hydroserver_url>`: The URL of the HydroServer.
* `<file_path>`: The path to the CSV file (You can provide any file path; it is not used for this operation).
