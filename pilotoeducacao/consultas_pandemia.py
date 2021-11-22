#!/usr/bin/env python
import argparse
import os, re, sys
import random
import time
import collections.abc
import pandas as pd
import json
import gspread
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials
from elasticsearch import Elasticsearch
from datetime import datetime
from pathlib import Path
from googleapiclient.discovery import build
from typing import Sequence, Optional

consultas = [
    {
        "primary_term": "internet",
        "secondary_terms": [
            ["via rádio", "banda larga", "fibra ótica", "ADSL"],
            ["educação remota", "ensino remoto", "ensino à distância"]
        ]
    }, {
        "primary_term": "educação",
        "secondary_terms": [
            ["ensino", "home school", "aprendizagem"],
            ["superensino", "Cogna", "liber", "Brainy","Stoodi"]
        ]
    }
]

consultas_bkp = [
    {
        "primary_term": "pandemia",
        "secondary_terms": [
            ["educação", "educacional", "ensino", "aprendizagem", "aprendizado", "home school", "home schooling"],
            ["em casa", "domiciliar", "à distância", "remota", "remoto", "virtual", "internet"]
        ]
    }, {
        "primary_term": "educação",
        "secondary_terms": [
            ["educação",  "ensino", "aprendizagem", "aprendizado", "home school", "home schooling"],
            #["internet", "em casa", "domiciliar", "à distância", "remota", "remoto", "virtual"],
            ["superensino", "Eduedu", "eduplay", "Cogna", "liber", "Brainy","Stoodi"]
        ]
    }, {
        "primary_term": "internet",
        "secondary_terms": [
            ["via rádio", "banda larga", "fibra ótica", "satélite", "ADSL", "3G", "4G", "5G"],
            ["educação remota", "ensino remoto", "ensino à distância", "ensino remoto emergencial"]
        ]
    }, {
        "primary_term": "internet",
        "secondary_terms": [
            ["em casa", "domiciliar", "à distância", "remota", "remoto", "virtual"],
            ["superensino", "Eduedu", "eduplay", "Cogna", "liber", "Brainy","Stoodi"]
        ]
    }
]

# Search indexes available
index_2018_2019 = "qd1819"
index_2020_2021 = "qd2021"

# TODO: call method that runs searches for each time frame (index) and saves results to a provided sheet.
def main():
    # Define here the index to be used
    index_to_query = index_2020_2021
    # Connect to ES instance
    es = None
    try:
        es = Elasticsearch(hosts=['localhost'], timeout=3000)
        print("Connected to %s" % str(es))
    except:
        print("Impossível conectar ao ElasticSearch. Will now exit.")
        exit(0)

    # Setup to write output to google sheets
    # define sheet from id
    planilha_1819_id = "1-IDLMx4V9I9RzKnyOVNgTtxpEw7SSp9i1E_iR1Bd53U"
    planilha_2021_id = "1kSpnIAViCObMmQmuAVp8Y77a9YvjTTOOz5LF9jVx_Sk"
    # select the spreeadsheet to be used:
    spreadsheet_key = planilha_2021_id
    scope = ['https://spreadsheets.google.com/feeds']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('./process/gcreds.json', scope)
    google_creds = gspread.authorize(credentials)


    for consulta in consultas:
        # Lists for keeping results that will be converted to dataframe
        index_list, primary_terms_list, secondary_terms_list, highlights_list, territory_name_list, urls_list = [], [], [], [], [], []
        print("Consulta : %s" % consulta)
        query = assembleIntervalQuery(consulta["primary_term"], consulta["secondary_terms"])
        print("Consulta montada : %s" % query)
        start_time = time.time()
        print(f'Busca Iniciada para %s' % index_to_query)
        results = es.search(
            index=index_to_query,
            body=query,
        )

        end_time = time.time() - start_time

        if results:
            n_hits = return_hits(results)
            print(f'→→→ Busca finalizada demorou %d segundos para retornar %d itens.' % (int(end_time), n_hits))
            highlights = return_highlights(results)
            cidades = return_territory_name(results)
            urls = return_urls(results)

            for n in range(0, n_hits):
                index_list.append(n)
                primary_terms_list.extend([consulta['primary_term']])
                secondary_terms_list.extend([str(consulta['secondary_terms'])])
                try:
                    highlights_list.extend(highlights[n])
                except:
                    pass
                try:
                    territory_name_list.extend([cidades[n]])
                except:
                    pass
                try:
                    urls_list.extend([urls[n]])
                except:
                    pass
            # Convert list of responses to dataframe
            responses_df = pd.DataFrame( list(zip( primary_terms_list, secondary_terms_list, urls_list, highlights_list, territory_name_list)), columns=['termo_principal', 'termos_complementares', 'url', 'excerto', 'cidade'], index=index_list)
            goiania = responses_df[responses_df['cidade'] == 'Goiânia']
            if goiania.size > 0:
                print(f'Updating spreadsheets with %d new items for Goiânia' % goiania.size)
                try:
                    # Workspace object
                    wks = google_creds.open_by_key(spreadsheet_key).sheet1
                    response = gd.set_with_dataframe(wks, goiania)
                    print(f'Response: %s' % response)
                    pass
                except:
                    print("Something went wrong with Spreasheets upload.")
            manaus = responses_df[responses_df['cidade'] == 'Manaus']
            if manaus.size > 0:
                print(f'Updating spreadsheets with %d new items for Manaus' % manaus.size)
                try:
                    # Workspace object
                    wks = google_creds.open_by_key(spreadsheet_key).sheet1
                    response = gd.set_with_dataframe(wks, manaus)
                    print(f'Response: %s' % response)
                    pass
                except:
                    print("Something went wrong with Spreasheets upload.")
            rio_de_janeiro = responses_df[responses_df['cidade'] == 'Rio de Janeiro']
            if rio_de_janeiro.size > 0:
                print(f'Updating spreadsheets with %d new items for Rio de Janeiro' % rio_de_janeiro.size)
                try:
                    # Workspace object
                    wks = google_creds.open_by_key(spreadsheet_key).sheet1
                    response = gd.set_with_dataframe(wks, rio_de_janeiro)
                    print(f'Response: %s' % response)
                    pass
                except:
                    print("Something went wrong with Spreasheets upload.")
    pass

def return_highlights(results: dict) -> list:

    """returns highlights"""

    if return_hits(results):
        return [
            doc["highlight"]["source_text"] for doc in results["hits"]["hits"]
        ]

def return_territory_name(results: dict) -> list:

    """returns territory name"""

    if return_hits(results):
        return [ doc["_source"]["territory_name"] for doc in results["hits"]["hits"] ]

def return_urls(results: dict) -> list:

    """returns url"""

    if return_hits(results):
        return [ doc["_source"]["url"] for doc in results["hits"]["hits"] ]

def return_hits(results: dict) -> list:

    return results["hits"]["total"]["value"]

def assembleIntervalQuery(primary_term: str, secondary_terms: []):

    """Receives the string parameters to be used as search terms and builds the interval query based on them with appropriate hardcoded parameters as highlights"""

    any_of_block = {
        "any_of" : {
        "intervals" : []
        }
    }
    assembled_query = {
        "_source": ["url", "territory_name"],
        "query": {
            "intervals" : {
                "source_text" : {
                    "all_of" : {
                        "intervals" : [
                            {
                                "match" : {
                                    "query" : primary_term
                                }
                            }
                        ],
                        "max_gaps" : 100
                    }
                }
            }
        },
        "size": 10000,
        "highlight": {
              "type" : "unified",
              "fragment_size": 300,
              "number_of_fragments" : 3,
              "fields": {
                    "source_text": {}
                    }
      }
    }
    # This should hava a list of strings for each any_of block to be added
    for terms in secondary_terms:
      # Adds an entire secondary term block
      for term in terms:
          match = None
          block_terms = term.split()
          # multiple terms for a single query item  Ex.: "banda larga"
          if len(block_terms) > 1:
              match = { "match" : { "query" : term, "ordered": True, "max_gaps": 0 } },
          # single term query item. Ex.: "internet"
          elif len(block_terms) == 1:
              match  = { "match" : { "query" : term } },
          # Add to the any_of_block to be added
          any_of_block["any_of"]["intervals"].extend(match)
      # Add block to the assembled_query
      assembled_query["query"]["intervals"]["source_text"]["all_of"]["intervals"].append(any_of_block)
    # Return the assembled query to be run
    return assembled_query

if __name__ == "__main__":
    main()
