
import httpx
import pandas as pd
import argparse
import logging
import asyncio


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# upload stations function
def upload_things(username,password,hydroserver_url,file_path):
    auth=(username,password)
    logger.info(f"Uploading stations to HydroServer: {hydroserver_url}, using file: {file_path}")
    things_endpoint = f"{hydroserver_url}/api/data/things"
    df = pd.read_csv(file_path)
    df['station_name'] = df['name']
    # breakpoint()
    results = df.apply(make_thing_post_request,args=(things_endpoint,auth,), axis=1)

    pass

#helper function to make the http post request 
def make_thing_post_request(row,hydroserver_url,auth):
    request_body={
        'latitude': row.latitude,
        'longitude': row.longitude,
        'elevation_m': row.elevation_m,
        'elevationDatum': row.elevationDatum,
        'state': row.state,
        'county': row.county,
        'name': row.station_name,
        'description': row.description,
        'samplingFeatureType': row.samplingFeatureType,
        'samplingFeatureCode': row.samplingFeatureCode,
        'siteType': row.siteType,
        'dataDisclaimer': row.dataDisclaimer,
    }
    # print(request_body)
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }
    # breakpoint()
    response = httpx.post(hydroserver_url, headers=headers, json=request_body, auth=auth)    
    logger.info(f"Uploading station {row.name} to HydroServer: {hydroserver_url}, Status Code {response.status_code}")
 

def delete_all_things(username,password,hydroserver_url):
    auth=(username,password)
    logger.info(f"Deleting stations from HydroServer: {hydroserver_url}")
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }
    things_list_json = httpx.get(hydroserver_url, headers=headers, auth=auth)

    for thing in things_list_json.json():
        # data = {'thing_id': thing['id']}
        thing_id = thing['id']
        # breakpoint()
        deleted_thing_response = httpx.delete(f'{hydroserver_url}/{thing_id}',auth=auth)
        logger.info(f"Deleting station {thing['name']} from HydroServer: {hydroserver_url}, Status Code {deleted_thing_response.status_code}")
    pass

def delete_all_datastreams(username,password,hydroserver_url):
    auth=(username,password)
    logger.info(f"Deleting data streams from HydroServer: {hydroserver_url}")
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }
    datastream_list_json = httpx.get(hydroserver_url, headers=headers, auth=auth)

    for datastream in datastream_list_json.json():
        datastream_id = datastream['id']
        # breakpoint()
        deleted_thing_response = httpx.delete(f'{hydroserver_url}/{datastream_id}',auth=auth)
        logger.info(f"Deleting station {datastream['name']} from HydroServer: {hydroserver_url}, Status Code {deleted_thing_response.status_code}")
    pass


async def upload_observations(username,password,hydroserver_url,file_path,date_column,value_column):
    list_async_task = []

    auth=(username,password)
    logger.info(f"Uploading observations to HydroServer: {hydroserver_url}, using file: {file_path}")

    df = pd.read_csv(file_path)
    # breakpoint()
    list_observations = df[[date_column, value_column]].values.tolist()
    chunk_size = 10000
    list_chunks = chunk_list(list_observations, chunk_size)

    datastream_id_list=df['datastream_id'].unique()
    if len (datastream_id_list) > 1:
        logger.info(f"there is more than one data streamer please correct, and only provide one,{datastream_id_list}")
        return 
    datastream_id = datastream_id_list[0]
    for chunk in list_chunks:
        task_post_observations = asyncio.create_task(
            make_observations_post_request_async(datastream_id, chunk, hydroserver_url, auth)
        )
        list_async_task.append(task_post_observations)
    results = await asyncio.gather(*list_async_task)
    return results

    # make_observations_post_request_async(datastream_id,list_observations,hydroserver_url,auth)



async def make_observations_post_request_async(datastream_id, list_observations, hydroserver_url, auth):
    # mssge_string = "uploading data for {datastream_id}"
    post_body = [
        {
            'Datastream': {
                '@iot.id': datastream_id
            },
            'components': ['phenomenonTime', 'result'],
            'dataArray': list_observations
        }
    ]

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(hydroserver_url, headers=headers, json=post_body, auth=auth, timeout=None)
            logger.info(f"Uploading observations for data stream {datastream_id} to HydroServer: {hydroserver_url}, error code: {response.status_code} error:{response.text}")
    except httpx.HTTPError as exc:
        print(f"Error while requesting {exc.request.url!r}.")
        logger.error(f"Failing Uploading observations for data stream {datastream_id} to HydroServer: {hydroserver_url}, error code: {response.status_code} error:{exc}")

    except Exception as e:
        logger.error(f"Failing Uploading observations for data stream {datastream_id} to HydroServer: {hydroserver_url}, error:{e}")


def upload_observations_sync(username,password,hydroserver_url,file_path,date_column,value_column,datastream_id):

    auth=(username,password)
    logger.info(f"Uploading observations to HydroServer: {hydroserver_url}, using file: {file_path}")
    # breakpoint()
    # file_path = file_path.replace('/edit#gid=', '/export?format=csv&gid=')
    df = pd.read_csv(file_path)
    # breakpoint()
    list_observations = df[[date_column, value_column]].values.tolist()
    chunk_size = 10000
    list_chunks = chunk_list(list_observations, chunk_size)
    api_endpoint = f"{hydroserver_url}/api/sensorthings/v1.1/Observations"        

    # datastream_id_list=df['datastream_id'].unique()
    # if len (datastream_id_list) > 1:
    #     logger.info(f"there is more than one data streamer please correct, and only provide one,{datastream_id_list}")
    #     return 
    # datastream_id = datastream_id_list[0]

    for chunk in list_chunks:
        make_observations_post_request(datastream_id, chunk, api_endpoint, auth)
        logger.info(f"uploading,chunk of {len(chunk)}")

#helper function to make the http post request 
def make_observations_post_request(datastream_id,list_observations,api_endpoint,auth):
    post_body = [
        {
            'Datastream': {
                '@iot.id': datastream_id
            },
            'components': ['phenomenonTime', 'result'],
            'dataArray': list_observations
        }
    ]

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }
    # breakpoint()
    response = httpx.post(api_endpoint, headers=headers, json=post_body, auth=auth,timeout=None)
    # logger.info(f"{response.text}")

    logger.info(f"Uploading observations for data stream {datastream_id} to HydroServer: {api_endpoint}, Status Code {response.status_code}")


def upload_datastreams(username,password,hydroserver_url,file_path):
    auth=(username,password)
    logger.info(f"Uploading datastreams to HydroServer: {hydroserver_url}, using file: {file_path}")

    df = pd.read_csv(file_path)
    df['name_datastream'] = df['name']
    # breakpoint()
    results = df.apply(make_datastream_post_request,args=(hydroserver_url,auth,), axis=1)

    pass


#helper function to make the http post request 
def make_datastream_post_request(row,hydroserver_url,auth):
    request_body={
        "name": row.name_datastream,
        "description": row.description,
        "observationType": row.observationType,
        "sampledMedium": row.sampledMedium,
        "noDataValue": row.noDataValue,
        "aggregationStatistic": row.aggregationStatistic,
        "timeAggregationInterval": row.timeAggregationInterval,
        "status": None,
        "resultType": row.resultType,
        "valueCount": None,
        "intendedTimeSpacing": row.intendedTimeSpacing,
        "phenomenonBeginTime": None,
        "phenomenonEndTime": None,
        "resultBeginTime": None,
        "resultEndTime": None,
        "dataSourceId": None,
        "dataSourceColumn": None,
        "isVisible": row.isVisible,
        "thingId": row.thingId,
        "sensorId": row.sensorId,
        "observedPropertyId": row.observedPropertyId,
        "processingLevelId": row.processingLevelId,
        "unitId": row.unitId,
        "timeAggregationIntervalUnitsId": row.timeAggregationIntervalUnitsId,
        "intendedTimeSpacingUnitsId": None
    } 

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }
    # breakpoint()
    response = httpx.post(hydroserver_url, headers=headers, json=request_body, auth=auth)
    if response.status_code == 403:
        breakpoint()
    logger.info(f"Uploading datastream {row.name} to HydroServer: {hydroserver_url}, Status Code {response.status_code}")


def chunk_list(input_list, chunk_size):
    # Initialize an empty list to store the smaller chunks
    chunks = []

    # Iterate through the input_list in steps of chunk_size
    for i in range(0, len(input_list), chunk_size):
        # Append a chunk of the list to the result list
        chunk = input_list[i:i + chunk_size]
        chunks.append(chunk)

    return chunks
def main():
    parser = argparse.ArgumentParser(description="Command-line tool for uploading stations, time series, and datastreams")

    # Add subparsers for each function
    subparsers = parser.add_subparsers(title="Subcommands", dest="subcommand")

    # Subparser for uploading stations
    parser_upload_stations = subparsers.add_parser("upload-things", help="Upload stations")
    parser_upload_stations.add_argument("username", help="HydroServer Username")
    parser_upload_stations.add_argument("password", help="HydroServer Password")    
    parser_upload_stations.add_argument("hydroserver_url", help="HydroServer URL")
    parser_upload_stations.add_argument("file_path", help="File path")
    parser_upload_stations.set_defaults(func=upload_things)

    # Subparser for uploading observations
    parser_upload_observations = subparsers.add_parser("upload-observations", help="Upload observations")
    parser_upload_observations.add_argument("username", help="HydroServer Username")
    parser_upload_observations.add_argument("password", help="HydroServer Password")    
    parser_upload_observations.add_argument("hydroserver_url", help="HydroServer URL")
    parser_upload_observations.add_argument("file_path", help="File path")
    parser_upload_observations.add_argument("date_column", help="Date Column")    
    parser_upload_observations.add_argument("value_column", help="Value column")
    parser_upload_observations.add_argument("datastream_id", help="DataStreamer ID")    


    parser_upload_observations.set_defaults(func=upload_observations_sync)

    # Subparser for uploading datastreams
    parser_upload_datastreams = subparsers.add_parser("upload-datastreams", help="Upload datastreams")
    parser_upload_datastreams.add_argument("username", help="HydroServer Username")
    parser_upload_datastreams.add_argument("password", help="HydroServer Password")        
    parser_upload_datastreams.add_argument("hydroserver_url", help="HydroServer URL")
    parser_upload_datastreams.add_argument("file_path", help="File path")  
    parser_upload_datastreams.set_defaults(func=upload_datastreams)


    # Subparser for deleting all things
    parser_delete_stations = subparsers.add_parser("delete-all-things", help="Delete stations")
    parser_delete_stations.add_argument("username", help="HydroServer Username")
    parser_delete_stations.add_argument("password", help="HydroServer Password")    
    parser_delete_stations.add_argument("hydroserver_url", help="HydroServer URL")
    # parser_delete_stations.add_argument("file_path", help="File path")
    parser_delete_stations.set_defaults(func=delete_all_things)

    # Subparser for deleting all streamers
    parser_delete_stations = subparsers.add_parser("delete-all-datastreams", help="Delete datastreams")
    parser_delete_stations.add_argument("username", help="HydroServer Username")
    parser_delete_stations.add_argument("password", help="HydroServer Password")    
    parser_delete_stations.add_argument("hydroserver_url", help="HydroServer URL")
    # parser_delete_stations.add_argument("file_path", help="File path")
    parser_delete_stations.set_defaults(func=delete_all_datastreams)

    args = parser.parse_args()
    # breakpoint()

    if hasattr(args, "func"):
        if hasattr(args, "date_column") and hasattr(args, "value_column"):
            # asyncio.run(args.func(args.username, args.password, args.hydroserver_url, args.file_path,args.date_column,args.value_column))
            args.func(args.username, args.password, args.hydroserver_url, args.file_path,args.date_column,args.value_column, args.datastream_id)

        elif not hasattr(args, "file_path"):
            args.func(args.username, args.password, args.hydroserver_url)
        else:
            args.func(args.username, args.password, args.hydroserver_url, args.file_path)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
    # asyncio.run(main())
