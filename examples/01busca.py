import os, sys
from pathlib import Path

# load custom modules
module_path = os.path.abspath(os.path.join("."))
if module_path not in sys.path:
    sys.path.append(module_path)


import pilotoeducacao

import pandas as pd


def main():

    # definir root como path
    ROOT = Path().resolve()
    DATA = ROOT / "data"

    creds = ROOT / "tests/data/YOUR_CREDENTIALS.json"
    queries = pilotoeducacao.get_queries(str(creds))
    dt = pd.DataFrame(queries)
    print(dt.info())

    # definir path para queries
    queries_file = DATA / "queries.csv"

    if os.path.exists(queries_file):
        old_queries = pd.read_csv(queries_file)
        if not old_queries.equals(dt):
            dt.to_csv(queries_file, index=False)

    else:
        dt.to_csv(queries_file, index=False)


if __name__ == "__main__":
    main()
