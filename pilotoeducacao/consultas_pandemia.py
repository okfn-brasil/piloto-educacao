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
        "primary_term": "ambiente virtual de aprendizagem",
        "secondary_terms": [
            ['aplicativo', 'plataforma']
        ]
    },{
        "primary_term": "ensino",
        "secondary_terms": [
            ['aplicativo', 'plataforma', 'ambiente virtual']
        ]
    }, {
        "primary_term": "ensino",
        "secondary_terms": [
            ['aplicativo', 'plataforma', 'ambiente virtual'],
            ['em casa', 'domiciliar', 'à distância', 'remota', 'remoto', 'virtual',  'online', 'on-line']
        ]
    }, {
        "primary_term": "ensino",
        "secondary_terms": [
            ['tablet', 'mobile', 'smartphone', 'notebook', 'celular', 'computador', 'chromebook']
        ]
    }, {
        "primary_term": "ensino",
        "secondary_terms": [
            ['internet', 'dados', '3G', '4G', '5G', 'banda larga'],
            ['vivo', 'oi', 'tim', 'claro', 'nextel', 'correios', 'telecom']
        ]
    }, {
        "primary_term": "ensino",
        "secondary_terms": [
            ['tplink', 'intelbras']
        ]
    }, {
        "primary_term": "ensino",
        "secondary_terms": [
            ['internet móvel', 'dados móveis', '3G', '4G', '5G', 'via rádio', 'banda larga', 'fibra ótica', 'satélite', 'ADSL', 'bluetooth']
        ]
    }, {
        "primary_term": "ensino",
        "secondary_terms": [
            ['superensino', 'Eduedu', 'eduplay', 'Cogna', 'lemann', 'liber', 'Brainy', 'Stoodi', 'ânima', 'passei direto', 'superprof' 'google', 'microsoft']
        ]
    }, {
        "primary_term": "ensino",
        "secondary_terms": [
            ['classroom', 'zoom', 'youtube', 'khan academy', 'kahoot', 'moodle', 'office']
        ]
    }, {
        "primary_term": "ensino",
        "secondary_terms": [
            ['positivo', 'lenovo', 'samsung', 'LG', 'HP', 'acer']
        ]
    }, {
        "primary_term": "educação",
        "secondary_terms": [
            ['aplicativo', 'plataforma', 'ambiente virtual']
        ]
    }, {
        "primary_term": "educação",
        "secondary_terms": [
            ['aplicativo', 'plataforma', 'ambiente virtual'],
            ['em casa', 'domiciliar', 'à distância', 'remota', 'remoto', 'virtual',  'online', 'on-line']
         ]
    }, {
        "primary_term": "educação",
        "secondary_terms": [
            ['tablet', 'mobile', 'smartphone', 'notebook', 'celular', 'computador', 'chromebook']
        ]
    }, {
        "primary_term": "educação",
        "secondary_terms": [
            ['internet', 'dados', '3G', '4G', '5G', 'banda larga'], ['vivo', 'oi', 'tim', 'claro', 'nextel', 'correios', 'telecom']
        ]
    }, {
        "primary_term": "educação",
        "secondary_terms": [
            ['tplink', 'intelbras']
        ]
    }, {
        "primary_term": "educação",
        "secondary_terms": [
            ['internet móvel', 'dados móveis', '3G', '4G', '5G', 'via rádio', 'banda larga', 'fibra ótica', 'satélite', 'ADSL', 'bluetooth']
        ]
    }, {
        "primary_term": "educação",
        "secondary_terms": [
            ['classroom', 'zoom', 'youtube', 'khan academy', 'kahoot', 'moodle', 'office']
        ]
    }, {
        "primary_term": "educação",
        "secondary_terms": [
            ['positivo', 'lenovo', 'samsung', 'LG', 'HP', 'acer']
        ]
    }, {
        "primary_term": "educacional",
        "secondary_terms": [
            ['aplicativo', 'plataforma', 'ambiente virtual']
        ]
    }, {
        "primary_term": "educacional",
        "secondary_terms": [
            ['aplicativo', 'plataforma', 'ambiente virtual'],
            ['em casa', 'domiciliar', 'à distância', 'remota', 'remoto', 'virtual',  'online', 'on-line']
        ]
    }, {
        "primary_term": "educacional",
        "secondary_terms": [
            ['tablet', 'mobile', 'smartphone', 'notebook', 'celular', 'computador', 'chromebook']
        ]
    }, {
        "primary_term": "educacional",
        "secondary_terms": [
            ['internet', 'dados', '3G', '4G', '5G', 'banda larga'], ['vivo', 'oi', 'tim', 'claro', 'nextel', 'correios', 'telecom']
        ]
    }, {
        "primary_term": "educacional",
        "secondary_terms": [
            ['tplink', 'intelbras']
        ]
    }, {
        "primary_term": "educacional",
        "secondary_terms": [
            ['internet móvel', 'dados móveis', '3G', '4G', '5G', 'via rádio', 'banda larga', 'fibra ótica', 'satélite', 'ADSL', 'bluetooth']
        ]
    }, {
        "primary_term": "educacional",
        "secondary_terms": [
            ['superensino', 'Eduedu', 'eduplay', 'Cogna', 'lemann', 'liber', 'Brainy', 'Stoodi', 'ânima', 'passei direto', 'superprof' 'google', 'microsoft']
        ]
    }, {
        "primary_term": "educacional",
        "secondary_terms": [
            ['classroom', 'zoom', 'youtube', 'khan academy', 'kahoot', 'moodle', 'office']
        ]
    }, {
        "primary_term": "educacional",
        "secondary_terms": [
            ['positivo', 'lenovo', 'samsung', 'LG', 'HP', 'acer']
        ]
    }
]

# Search indexes available
index_2018_2019 = "qd1819"
index_2020_2021 = "qd2021"
index_qd_full = "queridodiario2"

def main():
    # Select HERE the index to be used to run the program
    index_to_query = index_2020_2021

    # Connect to ES instance
    es = None
    try:
        es = Elasticsearch(hosts=['localhost'], timeout=3000)
        print("Connected to %s" % str(es))
        pass
    except:
        print("Impossível conectar ao ElasticSearch. Will now exit.")
        exit(0)

    # Setup to write output to google spreadsheets
    # define sheet from id
    planilha_1819_id = "1VKnMCO21NygMLqIPAM21BrcslBgnGfUg7HId7keyS5E"
    planilha_2021_id = "13atpUVjN5Nu9dviVcmqhq7EXE0zYgFPVpVjWcvT1pk8"
    planilha_qd_full = "1h7AQ4JoqK3gFGMXIRO-HrS95hnfDlVPojnzOqGeYkgI"

    # Auto selects HERE the spreeadsheet to be used for writting results to, according to selected index:
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
    # Use credentials to fetch service
    service = build('sheets', 'v4', credentials=credentials)

    # Para cada uma das consultas da lista de consultas:
    for consulta in consultas:
        # Lists for keeping results that will be converted to dataframe, one index per response, for all lists, representing a single row in the spreadsheets
        index_list, query_index_list, primary_terms_list, secondary_terms_list, highlights_list, territory_name_list, urls_list = [], [], [], [], [], [], []

        # Assemble query from provided keywords
        query = assembleIntervalQuery(consulta["primary_term"], consulta["secondary_terms"])
        # Track run time for each query
        start_time = time.time()
        # Print the assembled_query in a pretty JSON format
        #pretty_query = json.dumps(query, indent=3)
        #print(f'Busca iniciada para a seguinte consulta: %s' % pretty_query)

        # Run search on ES
        results = es.search(
            index=index_to_query,
            body=query,
        )
        # Track end time for query
        end_time = time.time() - start_time
        if results:
            n_hits = return_hits(results)
            print(f'ø A busca demorou %d segundos para retornar (%d) itens.' % (int(end_time), n_hits))

            # From the returned results, extract highlights, city and urls
            highlights = return_highlights(results)
            cidades = return_territory_name(results)
            urls = return_urls(results)

            index_n = 0
            # Para cada documento
            for n in range(0, n_hits):
                try:
                    # Para cada highlight do documento
                    for h in highlights[n]:
                        # Limpa o highlight usandoquerido_diario_toolbox:
                        highlights_clean_text = remove_breaks(h)

                        # Ensure matches the primary term bool
                        primary_match = False

                        # Ensure only highlights WITH at least ONE of each secondary matches list is present. If any of them returns empty, set secondary_matches to False
                        secondary_matches = True

                        # Check primary term match
                        if highlights_clean_text.find(consulta['primary_term']) > 0:
                            primary_match = True

                        # Para cada termo de um conjunto de termos secundários, verifique se ocorre no highlight
                        for st in consulta['secondary_terms']:
                            # Multiple words
                            if len(st) > 1:
                                positive_counter = 0
                                # Para cada termo, verificar cada palavra
                                for word in st:
                                    if highlights_clean_text.find(word) > 0:
                                        # Increment counter
                                        positive_counter = positive_counter + 1
                                # Para esses termos any of, se não encontrou pelo menos um
                                if positive_counter == 0:
                                    secondary_matches = False

                            elif len(st) == 1:
                                word = st[0]
                                if highlights_clean_text.find(word) <= 0:
                                    secondary_matches = False

                        # Se o trecho de destaque atual contém termos primários e pelo menos um de cada grupo secundário, é válido
                        if primary_match and secondary_matches:
                            # Adiciona o índice atual à lista e incrementa
                            index_list.append(index_n)
                            index_n = index_n + 1

                            # Put the current document result index to the current query to group highlights
                            query_index_list.append(n)

                            # Add the query's primary term to the list
                            primary_terms_list.append(consulta['primary_term'])

                            # Add the secondary terms to the list as a list of string elements to be printed as a single cell
                            secondary_terms_list.append(str(consulta['secondary_terms']))

                            # Add highlights to the list of highlights
                            highlights_list.append(highlights_clean_text)

                            # Add the city name to the list of cities
                            territory_name_list.append(cidades[n])

                            # Add the DO's url
                            urls_list.append(urls[n])

                            pass
                except:
                    pass

        # Converte lista de respostas para o dataframe
        responses_df = pd.DataFrame( list(zip( query_index_list, primary_terms_list, secondary_terms_list, urls_list, highlights_list, territory_name_list)), columns=['indice', 'termo_principal', 'termos_complementares', 'url', 'excerto', 'cidade'], index=index_list)

        # Selecionar os resultados das cidades relevantes
        cidades_ref = ['Cuiabá', 'Goiânia', 'Manaus', 'Rio de Janeiro']

        # Retorna apenas as linhas do DF que pertençam a essas cidades
        city_subset = responses_df[responses_df['cidade'].isin(cidades_ref)]

        # Caso haja algum resultado
        if (city_subset.count()[0] > 0):
            # Ordena os resultados dentro do dataframe
            city_subset = (city_subset.sort_values('termos_complementares'))
            city_subset = (city_subset.sort_values('cidade'))
            city_subset = (city_subset.sort_values('indice'))
            # The list of elements to be uploaded to the sheets
            city_subset_list = city_subset.values.tolist()

            print(f'Updating spreadsheets with %d new items' % len(city_subset))
            try:
                # Envia os resultados para a planilha google em lotes de 500 resultados, de modo a evitar sobrecarregar a API do google e perder registros
                N_ITEMS_BY_SHARD = 500
                n_items = len(city_subset_list)

                # Muitos resultados, separa em fatias
                if  n_items > N_ITEMS_BY_SHARD:
                    n_shards = n_items%N_ITEMS_BY_SHARD
                    lower_limit = 0
                    upper_limit = 499
                    print("N_SHARDS: %d" % n_shards)
                    for n in range(1, n_shards):
                        shard_items = city_subset_list[lower_limit:upper_limit]
                        print("Elements to add: %d " % len(shard_items))
                        # Execute update operation to the spreadsheets with provided parameters of range, valueInputOption, insertDataOption and the city subset list as values.
                        res = service.spreadsheets().values().append(spreadsheetId=spreadsheet_key, range=f'resultados!A2:F2', valueInputOption=f'USER_ENTERED', insertDataOption="INSERT_ROWS", body= {f'values': shard_items}).execute()
                        print(f'Spreadsheets Response: \n%s' % json.dumps(res))
                        # Increment limits for next round
                        lower_limit = lower_limit + N_ITEMS_BY_SHARD
                        upper_limit = upper_limit + N_ITEMS_BY_SHARD
                        # Impose upper limit to n_items to avoid index overflowing
                        if upper_limit > n_items:
                            upper_limit = n_items
                # Poucos resultados, única fatia
                else:
                    res = service.spreadsheets().values().append(spreadsheetId=spreadsheet_key, range=f'resultados!A2:F2', valueInputOption=f'USER_ENTERED', insertDataOption="INSERT_ROWS", body= {f'values': city_subset_list}).execute()
                    print(f'Spreadsheets Response: \n%s' % json.dumps(res))
                pass
            except:
                print(f'Something went wrong with Spreasheets upload: %s.' %  sys.exc_info()[0])
                pass

            # Objeto para criar dataframe que vai receber as contagens para a aba "total"
            counts_obj = {
                f'termo_principal': [],
                f'termos_complementares': [],
                'cidade': [],
                'total': []
            }

            # secondary terms as string
            sec_terms_str_list = ""
            for st in consulta["secondary_terms"]:
                sec_terms_str_list = sec_terms_str_list + " " + str(st)

            # Para cada cidade, vamos registrar um objeto com os termos principais e secundários da consulta, a cidade e o total de documentos encontrados para aquela consulta
            for c in cidades_ref:
                subcity = city_subset[city_subset['cidade'] == c]
                # Number of rows for this dataframe
                n_itens = subcity.count()[0]
                counts_obj['termo_principal'].append(consulta["primary_term"])
                counts_obj['termos_complementares'].append(sec_terms_str_list)
                counts_obj['cidade'].append(c)
                counts_obj['total'].append(n_itens)

            # Gera um dataframe a partir do objeto
            counts_df = pd.DataFrame(counts_obj)

            # Converte o dataframe em lista para ser enviada como valores da planilha
            result_totals = counts_df.values.tolist()
            print(f'Updating spreadsheets totals with %d new lines' % len(result_totals))
            try:
                # Executa atualização da planilha com adição dos novos resultados ao final
                res = service.spreadsheets().values().append(spreadsheetId=spreadsheet_key, range=f'total!A2:D2', valueInputOption=f'USER_ENTERED', insertDataOption="INSERT_ROWS", body= {f'values': result_totals}).execute()
                print(f'Spreadsheets Response: \n%s' % json.dumps(res))
                pass
            except:
                print(f'Something went wrong with Spreasheets upload: %s.' %  sys.exc_info()[0])
                pass
            pass
        # Case uma cidade não tenha resultados, deve adicionar um registro com 0 contagens para a consulta, de modo a constar no relatório.
        else:
            try:
                primary_term_str = consulta["primary_term"]
                # Secondary terms aux
                secondary_terms_str = str(consulta['secondary_terms'])
                # Objeto para criar dataframe com as linhas para a aba de contagens "total"
                counts_obj = {
                    "termo_principal": [],
                    "termos_complementares": [],
                    "cidade": [],
                    "total": []
                }
                # Para cada cidade, adiciona uma linha com as informações nulas
                for c in cidades_ref:
                    counts_obj["termo_principal"].append(primary_term_str)
                    counts_obj["termo_complementares"].append(secondary_terms_str)
                    counts_obj["cidade"].append(c)
                    counts_obj["total"].append(0)
                # Cria DataFrame a partir do objeto
                counts_df = pd.DataFrame(counts_obj)
                # Lista a partir do DF
                result_totals = counts_df.values.tolist()
                # Atualiza a segunda aba da planilha com um resumo com as contagens para cada consulta, por cidade
                res = service.spreadsheets().values().append(spreadsheetId=spreadsheet_key, range=f'total!A2:D2', valueInputOption=f'USER_ENTERED', insertDataOption="INSERT_ROWS", body= {f'values': result_totals}).execute()
                print(f'Spreadsheets Response: \n%s' % json.dumps(res))
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
    When a secondary term is provided, it can be found either after or before the primary term in the text, never N words apart from it.
    There is an all_of rule, that contains a match rule where the primary term is evaluated for a match. All groups of words from secondary terms
    are added to the all_of block as a match in an individual any_of block, if there are multiple words, treated as synonyms. Or added as single match rules,
    inside the all_of clause and considered in AND condition with the primary term match. There must be at least one positive term for each secondary_terms,
    along with the primary_term to return a highlight/document as positive.
    Builds the interval query based on the terms along with appropriate hardcoded parameters, such as highlights"""

    # Query a ser montada e retornada para a execução, com parâmetros de campos a serem retornados nos resultados, bem como as configurações de highlight
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
            "fragment_size": 1500,
            "number_of_fragments" : 1000,
            "order": "score",
            "fields": {
                "source_text": {}
            }
        }
    }

    # Configura o bloco de parâmetros primários
    primary_term_split = primary_term.split()
    primary_term_block = None
    if len(primary_term_split) > 1:
        primary_term_block =  { "match" : { "query" : primary_term, "ordered": True, "max_gaps": 0 } }
    else:
        primary_term_block =  { "match" : { "query" : primary_term } }
        # Add it to the assembled_query obj
    assembled_query["query"]["intervals"]["source_text"]["all_of"]["intervals"].extend([primary_term_block])

    # Aqui, se há múltiplos termos deve-se gerar um bloco any_of para selecionar um dos termos. Se há apenas um termo, adiciona-se ao invés apenas um bloco match
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
            # Aqui, vamos adicionar individualmente os sinônimos. Ex.: ["wireless", "sem fio"]
            for term in terms:
                split_term = term.split()
                if len(split_term) > 1:
                    match = { "match" : { "query" : term, "ordered": True, "max_gaps": 0 } }
                elif len(split_term) == 1:
                    match = { "match" : { "query" : term} }
                # Adciona o match ao novo bloco any_of
                any_of_block["any_of"]["intervals"].extend([match])
            # Adiciona o bloco ao assembled_query
            assembled_query["query"]["intervals"]["source_text"]["all_of"]["intervals"].extend([any_of_block])
        # Se há apenas um termo, adicione-o como match, de acordo com o número de palavras. Ex.: ["à rádio"].
        elif len(terms) == 1:
            term = terms[0]
            # multiple terms for a single query item  Ex.: "banda larga"
            if len(term.split()) > 1:
                match = { "match" : { "query" : term, "ordered": True, "max_gaps": 0 } }
            # single term to query. Ex.: "rádio"
            else:
                match  = { "match" : { "query" : term } }
            assembled_query["query"]["intervals"]["source_text"]["all_of"]["intervals"].append(match)

    # Returna a consulta montada em formato JSON a ser executada
    return assembled_query

if __name__ == "__main__":
    main()
