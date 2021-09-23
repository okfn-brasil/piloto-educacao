import argparse
import os, sys
import pickle
import re
import requests
from pathlib import Path

# load custom modules
module_path = os.path.abspath(os.path.join("."))
if module_path not in sys.path:
    sys.path.append(module_path)


import pandas as pd

from pilotoeducacao.queries import put_results


def main():

    # define parser objects
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", nargs="?", default=9)
    args = parser.parse_args()

    # definir root como path
    folder = f"rodada{int(args.rodada):02d}"
    ROOT = Path().resolve()
    DATA = ROOT / "data" / folder

    # definir lista de resultados
    resultados = []
    arquivos = DATA.iterdir()
    arquivos = sorted(list(arquivos))
    arquivos = [
        arquivo for arquivo in arquivos
        if not re.search(r"csv|pdf", str(arquivo))
    ]

    for arquivo in arquivos:
        with open(arquivo, "rb") as handle:
            resultado = pickle.load(handle)
            resultados.append(resultado)

    termos = ["compras", "educação", "tecnologia"]
    dados = {
        "Exercício": ["Factual"] * 3 + ["Contrafactual"] * 3,
        "Termo": termos + termos[::-1],
        "Hits": [r["hits"]["value"] for r in resultados],
    }
    df = pd.DataFrame(data=dados)
    print(df)

    # calcular os grupos
    G1 = set(resultados[2]["ids"])
    G2 = set(resultados[0]["ids"]) - set(resultados[2]["ids"])

    G3 = set(resultados[5]["ids"])
    G4 = set(resultados[3]["ids"]) - set(resultados[5]["ids"])

    # definir grupos
    true_positive = len(G1 & G3)
    true_negative = len(G2 & G4)
    false_positive = len(G1 - G3)
    false_negative = len(G2 & G3)

    print(f"Matriz de Confusão")
    print(f"---------------------------")
    print(f"True Positives (TP):  {true_positive:05d}")
    print(f"True Negatives (TN):  {true_negative:05d}")
    print(f"False Positives (FP): {false_positive:05d}")
    print(f"False Negatives (FN): {false_negative:05d}")

    google_creds = str(ROOT / "tests/data/YOUR_CREDENTIALS.json")

    # HITS
    # range da planilha a substituir
    range_ = f"queries!F{3*int(args.rodada)-1}:F200"

    # definir corpo dos valores a subir
    value_range_body = {
        "majorDimension": "COLUMNS",
        "values": [
            [r["hits"]["value"] for r in resultados[:3]],
        ],
    }

    # imprimir resultados
    put_results(google_creds, range_, value_range_body)

    # MATRIZ DE CONFUSÃO
    # range da planilha a substituir
    range_ = f"resultados!A{1+int(args.rodada)}:G{1+int(args.rodada)}"

    # definir corpo dos valores a subir
    value_range_body = {
        "majorDimension": "ROWS",
        "values": [
            [
                args.rodada,
                true_positive,
                true_negative,
                false_positive,
                false_negative,
                round(true_positive / (true_positive + false_positive), 2),
                round(true_positive / (true_positive + false_negative), 2),
            ],
        ],
    }

    # imprimir resultados
    put_results(google_creds, range_, value_range_body)

    # transformar resultados em banco de dados
    dt = pd.json_normalize(resultados[2]["documents"])
    dt[["sort.score", "sort.id"]] = pd.DataFrame(
        dt["sort"].tolist(), index=dt.index
    )
    dt.head()

    # sortear e salvar em csv
    dt_amostrados = dt.sample(n=5, random_state=42)

    # salvar os pdfs
    diarios = dt_amostrados["_source.url"].to_list()
    for diario in diarios:
        filename = diario.split("/")[-1]
        if filename[-3:] != "pdf":
            filename = filename + ".pdf"

        response = requests.get(diario)
        with open(DATA / filename, "wb") as f:
            f.write(response.content)

    # salvar lista de amostrados
    dt_amostrados.to_csv(DATA / "diarios_amostrados.csv", index=False)


if __name__ == "__main__":
    main()
