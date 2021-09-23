import argparse
import pickle
import os, sys
from pathlib import Path

import pandas as pd
from tqdm.contrib import tzip

# load custom modules
module_path = os.path.abspath(os.path.join("."))
if module_path not in sys.path:
    sys.path.append(module_path)

from pilotoeducacao import search


def main():

    # define GLOBAL variables
    ROOT = Path().resolve()
    DATA = ROOT / "data"

    # define es info
    ES_HOST = "http://localhost:9200"
    INDEX_NAME = "queridodiario2"

    # define parser objects
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", nargs="?", default=50)
    parser.add_argument("--operator", nargs=3, default=["AND", "AND", "AND"])
    args = parser.parse_args()
    print(args.operator)
    # load search terms
    termos = pd.read_csv(DATA / "queries.csv")
    termos = termos[termos["rodada"] == args.rodada]

    # definir termos de busca
    termo_compras = termos.loc[
        termos["subconjunto"] == "Compras", "termo"
    ].to_string(index=False)

    termo_educacao = termos.loc[
        termos["subconjunto"] == "Educação", "termo"
    ].to_string(index=False)

    termo_tecnologia = termos.loc[
        termos["subconjunto"] == "Tecnologia", "termo"
    ].to_string(index=False)

    # instanciar cliente de busca
    search_client = search.create_search_client(ES_HOST, INDEX_NAME)

    # definir função de busca
    def executar_busca(termo, filter_ids=None, operator="AND"):

        busca = search_client.search(
            search.SimpleQueryFactory(
                term=termo, default_operator=operator
            ),
            size_pg=10000,
            filter_ids=filter_ids,
        )

        resultados = {
            "documents": busca.documents(),
            "hits": busca.result_query["hits"]["total"],
            "ids": busca.ids(),
        }

        return resultados

    # exercício factual [índices 1-3] e exercício contractual [índices 4-6]
    resultados01 = executar_busca(
        termo_compras,
        operator=args.operator[0]
    )
    resultados02 = executar_busca(
        termo_educacao,
        resultados01["ids"],
        operator=args.operator[1]
    )
    resultados03 = executar_busca(
        termo_tecnologia,
        resultados02["ids"],
        operator=args.operator[2]
    )
    resultados04 = executar_busca(
        termo_tecnologia,
        operator=args.operator[2]
    )
    resultados05 = executar_busca(
        termo_educacao,
        resultados04["ids"],
        operator=args.operator[1]
    )
    resultados06 = executar_busca(
        termo_compras,
        resultados05["ids"],
        operator=args.operator[0]
    )

    # organizar o armazenamento de todos os resultados
    arquivos = [f"resultados_{n:02d}.pickle" for n in range(1, 7)]
    resultados = [
        resultados01,
        resultados02,
        resultados03,
        resultados04,
        resultados05,
        resultados06,
    ]

    # criar folder para salvar resultados
    folder = f"rodada{int(args.rodada):02d}"
    try:
        os.mkdir(DATA / folder)
    except:
        pass

    # salvar resultados
    for arquivo, resultado in tzip(arquivos, resultados):
        with open(DATA / folder / arquivo, "wb") as handle:
            pickle.dump(resultado, handle)


# execute block
if __name__ == "__main__":
    main()
