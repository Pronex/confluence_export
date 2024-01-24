"""
Exports confluence cloud spaces
"""
# pylint: disable=line-too-long

import os
import sys
import time

import asyncio
from asgiref.sync import sync_to_async

from dotenv import load_dotenv
import structlog

from atlassian import Confluence
from pypdf import PdfWriter, PdfReader

log = structlog.get_logger()  # logging

# required environment variables from .env file
required_env_vars = [
    'URL',  # e.g. https://<your-domain>.atlassian.net/wiki
    'USERNAME',  # e.g. john.doe@<your-domain>.com
    'API_TOKEN',  # e.g. ATATT3xFfG...
    'SPACE_KEY'  # e.g. MYSPACE
]
optional_env_vars = ['LIMIT']

# load environment variables
if not load_dotenv() or not all(var in os.environ for var in required_env_vars):
    # check if all required environment variables are present
    log.error('Error loading environment variables')
    # find out which environment variables are missing
    missing_env_vars = [var for var in required_env_vars if var not in os.environ]
    log.error(f'Missing environment variables: {missing_env_vars}')
    sys.exit(1)

# set globals
SPACE_KEY = os.getenv('SPACE_KEY')
OUT_PATH = "output"
COMBINED_PDF_PATH = f"{OUT_PATH}/{SPACE_KEY}_export.pdf"

confluence = Confluence(
    url=os.getenv('URL'),
    username=os.getenv('USERNAME'),
    password=os.getenv('API_TOKEN'),
    api_version="cloud",
)


# generic functions
# decorator to log time taken by function
def log_time(func):
    """
    Decorator to log time taken by function

    Args:
        func (function): Function to decorate

    Returns:
        function: Decorated function
    """

    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        elapsed_time = end - start
        log.info(f"# Execution time: {str(int(elapsed_time/60))}m and {str(int(elapsed_time%60))}s.")
        return result

    return wrapper


# confluence functions
# get the ids of all pages in a space
def get_list_pages(pages):
    """Get list of pages

    Args:
        pages (list): list of cnfluence page objects

    Returns:
        list: list of page ids
    """
    id_list = [item.get('id') for item in pages]
    log.info(f'Found {len(id_list)} pages in space {SPACE_KEY}')
    return id_list


# get page
@sync_to_async
def get_page(page_id):
    """Get page

    Args:
        page_id (str): page id

    Returns:
        str: page content
    """
    content = confluence.export_page(page_id)
    return content


# get all pages asynchronously
def get_all_pages(page_ids):
    """Get all pages

    Args:
        page_ids (list): list of page ids

    Returns:
        list: list of page contents
    """
    loop = asyncio.get_event_loop()
    tasks = [get_page(page_id) for page_id in page_ids]
    results = loop.run_until_complete(asyncio.gather(*tasks))
    return results


# gather pages and save as pdf
@log_time
def save_pages_as_pdf(confluence_pages):
    """Save pages as PDF

    Args:
        confluence_pages (list): list of confluence page objects
    """
    # combined_pdf = PyPDF2.PdfFileWriter()
    combined_pdf = PdfWriter()

    # check if directory exists and create if not
    if not os.path.exists(OUT_PATH):
        os.makedirs(OUT_PATH)

    # get page ids
    page_ids = get_list_pages(confluence_pages)

    # get page contents as pdf bytes
    page_contents = get_all_pages(page_ids)

    # combine pdf bytes
    for page in page_contents:
        # temporarily save page as pdf to process it
        with open(f"{OUT_PATH}/tmp_page.pdf", "wb") as temp_pdf_file:
            temp_pdf_file.write(page)

        # add page to combined pdf
        combined_pdf.append(PdfReader(f"{OUT_PATH}/tmp_page.pdf"))

    # write out to combined pdf
    with open(COMBINED_PDF_PATH, "wb") as combined_pdf_file:
        combined_pdf.write(combined_pdf_file)

    # delete temporary file
    os.remove(f"{OUT_PATH}/tmp_page.pdf")

    log.info(f"Combined PDF saved to '{COMBINED_PDF_PATH}'")


# default entry point
if __name__ == "__main__":

    # get pages from space
    limit = int(os.getenv('LIMIT', '1000'))
    confluence_content = confluence.get_all_pages_from_space(SPACE_KEY,
                                                             start=0,
                                                             limit=limit,
                                                             status=None,
                                                             expand=None,
                                                             content_type='page')

    # check if limit is reached
    if len(confluence_content) == limit:
        log.warning(f'Limit of {limit} pages reached. Increase limit in .env file to export more pages.')

    # save pages as pdf
    save_pages_as_pdf(confluence_content)
    log.info('Done exporting pages')
