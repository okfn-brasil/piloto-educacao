import os
from collections import namedtuple
from pprint import pprint

import pandas as pd
from googleapiclient.discovery import build


def get_queries(google_creds):

    # define global variables
    SPREADSHEET_ID = "YOUR SPREADSHEET ID HERE"
    # RANGE_NAME = "queries!A1:J151"
    # use your range here

    # define google auth credentials
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_creds

    # build api service and return results
    service = build("sheets", "v4")
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME)

    # store results in and create named tuple
    terms = result.execute()["values"]
    queries = pd.DataFrame(columns=terms[0], data=terms[1:])

    # return call
    return queries


def put_results(google_creds, range_, value_range_body):

    # define global variables
    SPREADSHEET_ID = "YOUR SPREADSHEET ID HERE"

    # define google auth credentials
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_creds

    # build api service and return results
    service = build("sheets", "v4")
    sheet = service.spreadsheets()
    request = sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=range_,
        valueInputOption="RAW",
        body=value_range_body,
    )

    # execute query
    response = request.execute()

    # return call
    return pprint(response)
