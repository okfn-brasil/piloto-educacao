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
            ["aprendizagem", "aprendizado"],
            ["home school", "home schooling"],
            ["internet"],
            ["em casa", "domiciliar"],
            ["à distância"],
            ["remota", "remoto"],
            ["virtual"]
        ]
    }, {
        "primary_term": "ensino remoto emergencial",
        "secondary_terms": [
            ["equipamento"],
            ['aluno', 'aluna', 'alunas', 'alunos'],
            ['tablet', 'mobile', 'smartphone', 'notebook']
        ]
    }, {
        "primary_term": "ensino virtual",
        "secondary_terms": [
            ["dados móveis"],
            ["contrato", "provedor"],
            ['aluno', 'aluna', 'alunas', 'alunos']
        ]
    }, {
        "primary_term": "ensino",
        "secondary_terms": [
            ["remoto", "virtual"],
            ["superensino", "Eduedu", "eduplay", "Cogna", "liber", "Brainy", "Stoodi", "classroom"]
        ]
    }, {
        "primary_term": "educação",
        "secondary_terms": [
            ["ensino", "escola","remoto", "virtual"],
            ["internet móvel", "dados móveis", "3G", "4G", "5G"],
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
            ["Cogna", "Brainy", "Stoodi", "eduedu", "superensino"]
        ]
    }, {
        "primary_term": "ensino",
        "secondary_terms": [
            ["virtual"],
            ["classroom", "Cogna", "Brainy", "Stoodi", "eduedu", "superensino"]
        ]
    }, {
        "primary_term": "educação",
        "secondary_terms": [
            ["provedor"],
            ["Oi", "Claro", "Tim", "Vivo", "Telecom"]
        ]
    }, {
        "primary_term": "educação",
        "secondary_terms": [
            ["plataforma"],
            ["digital", "online", "on-line", "virtual"]
        ]
    }, {
        "primary_term": "internet",
        "secondary_terms": [
            ["via rádio", "banda larga", "fibra ótica", "satélite", "ADSL", "3G", "4G", "5G", "dados móveis"],
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
index_qd_full = "queridodiario2"

def main():
    # Select HERE the index to be used to tun the program
    index_to_query = index_qd_full
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
    planilha_1819_id = "1VKnMCO21NygMLqIPAM21BrcslBgnGfUg7HId7keyS5E"
    planilha_2021_id = "13atpUVjN5Nu9dviVcmqhq7EXE0zYgFPVpVjWcvT1pk8"
    planilha_qd_full = "1h7AQ4JoqK3gFGMXIRO-HrS95hnfDlVPojnzOqGeYkgI"

    # selects HERE the spreeadsheet to be used for writting results to, according to selected index:
    spreadsheet_key = None
    if index_to_query == index_2018_2019:
        spreadsheet_key = planilha_1819_id
        pass
    if index_to_query == index_2020_2021:
        spreadsheet_key = planilha_2021_id
    if index_to_query == index_qd_full:
        spreadsheet_key = planilha_qd_full
    # define the access scope allowed to the credentials
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    # fetch information from json file and process with defined scope
    credentials = ServiceAccountCredentials.from_json_keyfile_name('./process/gcreds.json', scope)
    print(str(credentials))
    # Use credentials to fetch service
    service = build('sheets', 'v4', credentials=credentials)

    # Para cada uma das consultas da lista de consultas:
    for consulta in consultas:
        # Lists for keeping results that will be converted to dataframe, one index per response, for all lists, representing a single row in the spreadsheets
        index_list, primary_terms_list, secondary_terms_list, highlights_list, territory_name_list, urls_list = [], [], [], [], [], []
        #print("Consulta : %s" % consulta)
        query = assembleIntervalQuery(consulta["primary_term"], consulta["secondary_terms"])
        start_time = time.time()
        # Print the assembled_query in a pretty JSON format
        pretty_query = json.dumps(query, indent=3)
        #print(f'Busca iniciada para a seguinte consulta: %s' % pretty_query)
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
                    highlights_text = []
                    # Fetch the first[0] highlight for this query/terms
                    for h in highlights[n]:
                        # Fix highlights blank chars:
                        highlights_text.append(remove_breaks(h))
                    # Concatenate all highlights into a single text, separated by double blank lines \n
                    separated_highlights = "\n\n".join(highlights_text)
                    # Add highlights to the list of highlights
                    highlights_list.append(separated_highlights)
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
            # Sort by city name and secondary terms and turn into list from df
            city_subset = (city_subset.sort_values('termos_complementares'))
            city_subset = (city_subset.sort_values('cidade'))
            # The list of elements to be uploaded to the sheets
            city_subset_list = city_subset.values.tolist()

            print(f'Updating spreadsheets with %d new items' % len(city_subset))
            try:
                # Upload in shards of up to 500 items # TODO: may not be necessary
                N_ITEMS_BY_SHARD = 500
                n_items = len(city_subset_list)
                # Treat cases where the amount of results to be added is greater than the shard limit
                if  n_items > N_ITEMS_BY_SHARD:
                    n_shards = n_items%N_ITEMS_BY_SHARD
                    lower_limit = 0
                    upper_limit = 499
                    print("N_SHARDS: %d" % n_shards)
                    for n in range(1, n_shards):
                        shard_items = city_subset_list[lower_limit:upper_limit]
                        print("Elements to add: %d " % len(shard_items))
                        print("Elements: %s" % shard_items)
                        # Execute update operation to the spreadsheets with provided parameters of range, valueInputOption, insertDataOption and the city subset list as values.
                        res = service.spreadsheets().values().append(spreadsheetId=spreadsheet_key, range=f'resultados!A2:E2', valueInputOption=f'USER_ENTERED', insertDataOption="INSERT_ROWS", body= {f'values': shard_items}).execute()
                        print(f'Response: %s' % res)
                        lower_limit = lower_limit + N_ITEMS_BY_SHARD
                        upper_limit = upper_limit + N_ITEMS_BY_SHARD
                        # Impose upper limit to n_items to avoid index overflowing
                        if upper_limit > n_items:
                            upper_limit = n_items
                else:
                    res = service.spreadsheets().values().append(spreadsheetId=spreadsheet_key, range=f'resultados!A2:E2', valueInputOption=f'USER_ENTERED', insertDataOption="INSERT_ROWS", body= {f'values': city_subset_list}).execute()
                    print(f'Response: %s' % res)
                pass
            except:
                print(f'Something went wrong with Spreasheets upload: %s.' %  sys.exc_info()[0])
                pass

            # New dataframe to hold the lines with totals
            counts_obj = {
                f'termo_principal': [],
                f'termos_complementares': [],
                'cidade': [],
                'total': []
            }

            for c in cidades_ref:
                subcity = city_subset[city_subset['cidade'] == c]
                n_itens = subcity.count()[0]
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

    """Receives the string parameters to be used as search terms. The terms might be a single or multi word value. When multiple words are provided,
    they will be treated as a full text string, or in other words, in the interval query synthax, as set to ordered = True and max_gaps = 0.
    When a single term is provided, it can be found either after or before the primary term.
    The primary term is evaluated for a match, and all groups of words from secondary terms are added as an individual any_of block,
    inside an all_of clause considered in AND condition with the primary term match.
    Builds the interval query based on the terms along with appropriate hardcoded parameters, such as highlights"""

    # Mounted query to be returned and run
    assembled_query = {
        "_source": ["url", "territory_name"],
        "query": {
            "intervals" : {
                "source_text" : {
                    "all_of" : {
                        "intervals" : [],
                        "max_gaps" : 100
                    }
                }
            }
        },
        # Max ammount of results to accept as response
        "size": 10000,
        "highlight": {
            "type" : "unified",
            "fragment_size": 500,
            "number_of_fragments" : 8,
            "fields": {
                "source_text": {}
            }
        }
    }

    # Set primary term parameters
    primary_term_split = primary_term.split()
    primary_term_block = None
    if len(primary_term_split) > 1:
        primary_term_block =  { "match" : { "query" : primary_term, "ordered": True, "max_gaps": 0 } }
    else:
        primary_term_block =  { "match" : { "query" : primary_term } }
        # Add it to the assembled_query obj
    assembled_query["query"]["intervals"]["source_text"]["all_of"]["intervals"].extend([primary_term_block])

    # Aqui, se há múltiplos termos deve-se gerar um block any_of a selecionar um dos dois termos. Se há apenas um termo, adiciona-se um match
    for terms in secondary_terms:
        match = None
        # Any_of block
        if len(terms) > 1:
            # Block to be added
            any_of_block = {
                "any_of" : {
                "intervals" : []
                }
            }
            # Multiple options to choose one from, add to new any_of block. Ex.: ["wireless", "sem fio"]
            # Aqui, vamos adicionar individualmente os sinônimos
            for term in terms:
                split_term = term.split()
                if len(split_term) > 1:
                    match = { "match" : { "query" : term, "ordered": True, "max_gaps": 0 } }
                elif len(split_term) == 1:
                    match = { "match" : { "query" : term} }
                # Add to the any_of_block to be added
                any_of_block["any_of"]["intervals"].extend([match])
            # Add block to the assembled_query
            assembled_query["query"]["intervals"]["source_text"]["all_of"]["intervals"].extend([any_of_block])
        # single term query item. Ex.: ["à rádio"]. Se há apenas um termo, adicione-o, de acordo com o número de palavras.
        elif len(terms) == 1:
            term = terms[0]
            # multiple terms for a single query item  Ex.: "banda larga"
            if len(term.split()) > 1:
                match = { "match" : { "query" : term, "ordered": True, "max_gaps": 0 } }
            # single term to query. Ex.: "rádio"
            else:
                match  = { "match" : { "query" : term } }
            assembled_query["query"]["intervals"]["source_text"]["all_of"]["intervals"].append(match)

    # Return the assembled query to be run
    return assembled_query

if __name__ == "__main__":
    main()
