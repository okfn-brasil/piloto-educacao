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
from querido_diario_toolbox.process.text_process import remove_breaks
from oauth2client.service_account import ServiceAccountCredentials
from elasticsearch import Elasticsearch
from datetime import datetime
from pathlib import Path
from googleapiclient.discovery import build
from typing import Sequence, Optional


consultas = [
    {
        "primary_term": "ensino",
        "secondary_terms": [
            ["aprendizagem", "aprendizado", "home school", "home schooling"],
            ["internet", "em casa", "domiciliar", "à distância", "remota", "remoto", "virtual"]
        ]
    }, {
        "primary_term": "ensino",
        "secondary_terms": [
            ["remoto", "virtual"],
            ["superensino", "Eduedu", "eduplay", "Cogna", "liber", "Brainy","Stoodi"]
        ]
    }, {
        "primary_term": "educação",
        "secondary_terms": [
            ["ensino", "escola","remoto", "virtual"],
            ["telefonia móvel", "3g", "4g", "5g"],
            ["contrato", "licitação", "fornecedor", "provedor"]
        ]
    }, {
        "primary_term": "ensino",
        "secondary_terms": [
            ["banda larga", "fibra ótica", "ADSL"],
            ["educação remota", "ensino remoto"]
        ]
    }, {
        "primary_term": "educação",
        "secondary_terms": [
            ["Cogna", "Brainy","Stoodi", "eduedu", "superensino"]
        ]
    }, {
        "primary_term": "ensino",
        "secondary_terms": [
            ["virtual", "Cogna", "Brainy", "Stoodi", "eduedu", "superensino"]
        ]
    }, {
        "primary_term": "educação",
        "secondary_terms": [
            ["provedor"],
            ["Oi", "Claro", "Tim", "Vivo"]
        ]
    }, {
        "primary_term": "educação",
        "secondary_terms": [
            ["plataforma"],
            ["digital", "online", "on-line", "virtual"]
        ]
    }, {
        "primary_term": "pandemia",
        "secondary_terms": [
            ["educação", "educacional", "ensino", "aprendizagem", "aprendizado", "home school", "home schooling"],
            ["em casa", "domiciliar", "à distância", "remota", "remoto", "virtual", "internet"]
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
    }, {
        "primary_term": "educação",
        "secondary_terms": [
            ["educação",  "ensino", "aprendizagem", "aprendizado", "home school", "home schooling"],
            ["internet", "em casa", "domiciliar", "à distância", "remota", "remoto", "virtual"],
            ["superensino", "Eduedu", "eduplay", "Cogna", "liber", "Brainy","Stoodi"]
        ]
    }
]

# Search indexes available
index_2018_2019 = "qd1819"
index_2020_2021 = "qd2021"

def main():
    # Select HERE the index to be used to tun the program
    index_to_query = index_2020_2021
    # Connect to ES instance
    es = None
    try:
        es = Elasticsearch(hosts=['localhost'], timeout=3000)
        print("Connected to %s" % str(es))
    except:
        print("Impossível conectar ao ElasticSearch. Will now exit.")
        exit(0)

    # Setup to write output to google spreadsheets
    # define sheet from id
    planilha_1819_id = "1-IDLMx4V9I9RzKnyOVNgTtxpEw7SSp9i1E_iR1Bd53U"
    planilha_2021_id = "1kSpnIAViCObMmQmuAVp8Y77a9YvjTTOOz5LF9jVx_Sk"

    # selects HERE the spreeadsheet to be used for writting results to, according to selected index:
    spreadsheet_key = None
    if index_to_query == index_2018_2019:
        spreadsheet_key = planilha_1819_id
        pass
    if index_to_query == index_2020_2021:
        spreadsheet_key = planilha_2021_id

    # define the access scope allowed to the credentials
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    # fetch information from json file and process with defined scope
    credentials = ServiceAccountCredentials.from_json_keyfile_name('./process/gcreds.json', scope)
    # Use credentials to fetch service
    service = build('sheets', 'v4', credentials=credentials)

    # Para cada uma das consultas da lista de consultas:
    for consulta in consultas:
        # Lists for keeping results that will be converted to dataframe, one index per response, for all lists, representing a single row in the spreadsheets
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
            print(f'→→→ Busca finalizada demorou %d segundos para retornar (%d) itens.' % (int(end_time), n_hits))
            # From the returned results, extract highlights, city and urls
            highlights = return_highlights(results)
            cidades = return_territory_name(results)
            urls = return_urls(results)

            for n in range(0, n_hits):
                # Increment index and append it to list
                index_list.append(n)
                # Add the query's primary term to the list
                primary_terms_list.extend([consulta['primary_term']])
                # Add the secondary terms to the list as a list of string elements to be printed as a single cell
                secondary_terms_list.extend([str(consulta['secondary_terms'])])
                try:
                    # Fetch the first[0] highlight for this query/terms
                    h = highlights[n][0]
                    # Fix highlights blank chars:
                    h = remove_breaks(h)
                    # Add highlights to the list of highlights
                    highlights_list.append(h)
                except:
                    pass
                try:
                    # Add the city name to the list of cities
                    territory_name_list.extend([cidades[n]])
                except:
                    pass
                try:
                    urls_list.extend([urls[n]])
                except:
                    pass

        # Convert list of responses to dataframe
        responses_df = pd.DataFrame( list(zip( primary_terms_list, secondary_terms_list, urls_list, highlights_list, territory_name_list)), columns=['termo_principal', 'termos_complementares', 'url', 'excerto', 'cidade'], index=index_list)
        # Selecionar os resultados das cidades relevantes
        cidades_ref = ['Goiânia', 'Manaus', 'Rio de Janeiro']
        city_subset = responses_df[responses_df['cidade'].isin(cidades_ref)]
        # Caso haja algum resultado
        if city_subset.size > 0:
            # new dataframe to hold the lines with totals
            counts_obj = {
                f'termo_principal': [],
                f'termos_complementares': [],
                'cidade': [],
                'total': []
            }
            for c in cidades_ref:
                subcity = city_subset[city_subset['cidade'] == c]
                n_itens = subcity.size
                counts_obj['termo_principal'].append(consulta["primary_term"])
                counts_obj['termos_complementares'].append(str(consulta["secondary_terms"]))
                counts_obj['cidade'].append(c)
                counts_obj['total'].append(n_itens)
            counts_df = pd.DataFrame(counts_obj)
            # Adiciona à segunda aba da planilha um resumo com as contagens para cada consulta, por cidade
            result_totals = counts_df.values.tolist()
            print(f'Updating spreadsheets totals with %d new lines' % len(result_totals))
            try:
                # Execute update operation to the spreadsheets with provided parameters of range, valueInputOption, insertDataOption and the city subset list as values.
                res = service.spreadsheets().values().append(spreadsheetId=spreadsheet_key, range=f'total!A2:D2', valueInputOption=f'USER_ENTERED', insertDataOption="INSERT_ROWS", body= {f'values': result_totals}).execute()
                print(f'Response: %s' % res)
                pass
            except:
                print(f'Something went wrong with Spreasheets upload: %s.' %  sys.exc_info()[0])
                pass

            # Sort by city name and secondary terms and turn into list from df
            city_subset = (city_subset.sort_values('termos_complementares'))
            city_subset = (city_subset.sort_values('cidade'))
            city_subset_list = city_subset.values.tolist()

            print(f'Updating spreadsheets with %d new items' % len(city_subset))
            try:
                # Execute update operation to the spreadsheets with provided parameters of range, valueInputOption, insertDataOption and the city subset list as values.
                res = service.spreadsheets().values().append(spreadsheetId=spreadsheet_key, range=f'resultados!A2:E2', valueInputOption=f'USER_ENTERED', insertDataOption="INSERT_ROWS", body= {f'values': city_subset_list}).execute()
                print(f'Response: %s' % res)
                pass
            except:
                print(f'Something went wrong with Spreasheets upload: %s.' %  sys.exc_info()[0])
                pass
    pass

def return_highlights(results: dict) -> list:

    """returns all highlights from response documents"""

    if return_hits(results):
        return [
            doc["highlight"]["source_text"] for doc in results["hits"]["hits"]
        ]

def return_territory_name(results: dict) -> list:

    """returns territory name list from the documents"""

    if return_hits(results):
        return [ doc["_source"]["territory_name"] for doc in results["hits"]["hits"] ]

def return_urls(results: dict) -> list:

    """returns url list for the documents """

    if return_hits(results):
        return [ doc["_source"]["url"] for doc in results["hits"]["hits"] ]

def return_hits(results: dict) -> list:

    """returns total number of hits for the query"""

    return results["hits"]["total"]["value"]

def assembleIntervalQuery(primary_term: str, secondary_terms: []):

    """Receives the string parameters to be used as search terms. The terms might be a single or multi word value. When multiple words are provided, they
    will be treated as a full text string, or in other words, in the interval query synthax, as set to ordered = True and max_gaps = 0.
    When a single term is provided, it can be found either after or before the primary term.
    The primary term is evaluated for a match, and all groups of words from secondary terms are added as an individual any_of block, inside an all_of clause considered in AND condition with the primary term match.
    Builds the interval query based on the terms along with appropriate hardcoded parameters, such as highlights"""

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
                        "max_gaps" : 50
                    }
                }
            }
        },
        # Max ammount of results to accept as response
        "size": 10000,
        "highlight": {
            "type" : "unified",
            "fragment_size": 1500,
            "number_of_fragments" : 1,
            "fields": {
                "source_text": {}
            }
        }
    }
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
