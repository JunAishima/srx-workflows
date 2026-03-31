import traceback

from prefect import task, flow, get_run_logger
from prefect.blocks.notifications import SlackWebhook
from prefect.context import FlowRunContext

from xanes_exporter import xanes_exporter
from xrf_hdf5_exporter import xrf_hdf5_exporter
from logscan import logscan
from dotenv import load_dotenv
import os
from data_validation import get_run

CATALOG_NAME = "srx"

@task(log_prints=True)
def get_api_key_from_env():
    logger = get_run_logger()
    contents = os.listdir("/srv")
    for content in contents:
        logger.info(f"/srv: {content}")
    with open("/srv/container.secret", "r") as secrets:
        load_dotenv(stream=secrets)
    api_key = os.environ["TILED_API_KEY"]
    return api_key


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow
def end_of_run_workflow(stop_doc, api_key=None, dry_run=False):
    logger = get_run_logger()
    uid = stop_doc["run_start"]
    if api_key:
        logger.info(f"api_key: first 5: {api_key[:5]}")
    else:
        logger.info("end_of_run_workflow 1: No API key")
    api_key = get_api_key_from_env()

    # data_validation(uid, return_state=True, api_key=api)
    xanes_exporter(uid, api_key=api_key, dry_run=dry_run)
    xrf_hdf5_exporter(uid, api_key=api_key, dry_run=dry_run)
    logscan(uid, api_key=api_key, dry_run=dry_run)
    log_completion()
