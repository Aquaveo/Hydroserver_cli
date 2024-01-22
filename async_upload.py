import asyncio
import pandas as pd

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def chunk_list(input_list, chunk_size):
    # Initialize an empty list to store the smaller chunks
    chunks = []

    # Iterate through the input_list in steps of chunk_size
    for i in range(0, len(input_list), chunk_size):
        # Append a chunk of the list to the result list
        chunk = input_list[i:i + chunk_size]
        chunks.append(chunk)

    return chunks

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