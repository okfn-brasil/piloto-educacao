import json
from elasticsearch import Elasticsearch


def create_search_client(
    es_host,
    index_name,
):
    return SearchClient(Elasticsearch([es_host], timeout=300), index_name)


class QueryFactory:
    def create_query(self) -> dict:
        """Create a query"""
        pass


# https://www.elastic.co/guide/en/elasticsearch/reference/7.14/query-dsl-simple-query-string-query.html
class SimpleQueryFactory(QueryFactory):
    def __init__(self, term, default_operator="OR"):
        self.term = term
        self.default_operator = default_operator

    def create_query(self):
        return {
            "simple_query_string": {
                "query": self.term,
                "fields": ["source_text"],
                "default_operator": self.default_operator,
            }
        }


# https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase.html
class MatchPhraseQueryFactory(QueryFactory):
    def __init__(self, term):
        self.term = term

    def create_query(self):
        return {"match_phrase": {"source_text": self.term}}


# https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-regexp-query.html
class RegexQueryFactory(QueryFactory):
    def __init__(self, pattern, flags="ALL", case_insensitive=True):
        self.pattern = pattern
        self.flags = flags
        self.case_insensitive = case_insensitive

    def create_query(self):
        return {
            "regexp": {
                "source_text": {
                    "value": self.pattern,
                    "flags": self.flags,
                    "case_insensitive": self.case_insensitive,
                }
            }
        }


# https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-fuzzy-query.html
class FuzzyQueryFactory(QueryFactory):
    def __init__(
        self, term, fuzziness="AUTO", max_expansions=50, prefix_length=0
    ):
        self.term = term
        self.fuzziness = fuzziness
        self.max_expansions = max_expansions
        self.prefix_length = prefix_length

    def create_query(self):
        return {
            "fuzzy": {
                "source_text": {
                    "value": self.term,
                    "fuzziness": self.fuzziness,
                    "max_expansions": self.max_expansions,
                    "prefix_length": self.prefix_length,
                }
            }
        }


class SearchResult:
    def __init__(self, result_query):
        self.result_query = result_query

    def __hits(self):
        return self.result_query["hits"]["total"]["value"]

    def documents(self):
        return self.result_query["hits"]["hits"]

    def ids(self):
        return list(map(lambda doc: doc["_id"], self.documents()))

    def show(self):
        print("Got %d Hits:" % self.__hits())
        for hit in self.documents():
            print(hit)


class SearchClient:
    def __init__(self, elasticsearch_client, index_name):
        self.elasticsearch_client = elasticsearch_client
        self.index_name = index_name

    def search(self, query_factory, from_pg=0, size_pg=10000, filter_ids=None):
        custom_query = (
            {
                "bool": {
                    "must": query_factory.create_query(),
                    "filter": {"ids": {"values": filter_ids}},
                }
            }
            if filter_ids
            else query_factory.create_query()
        )

        full_query = {
            "track_total_hits": True,
            "track_scores": True,
            "_source": True,
            "sort": ["_score", {"date": "desc"}],
            "from": from_pg,
            "size": size_pg,
            "query": custom_query,
        }
        print(f"full_query {json.dumps(full_query)}")
        return SearchResult(
            self.elasticsearch_client.search(
                index=self.index_name,
                body=full_query,
                _source_excludes=["source_text"],
            )
        )
