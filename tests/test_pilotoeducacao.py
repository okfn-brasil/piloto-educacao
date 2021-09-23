#!/usr/bin/env python

"""Tests for `pilotoeducacao` package."""


import unittest

import pilotoeducacao


class TestPilotoeducacao(unittest.TestCase):
    """Tests for `pilotoeducacao` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_query_retrieval(self):
        """test retrieval of google sheet information"""

        creds = "tests/data/okbr-324423-170efa3842c2.json"
        queries = pilotoeducacao.get_queries(creds)

        # define fields in query spreadsheet
        fields = ("termo", "tipo", "escopo", "resultado")

        # test if queries have all fields and query term is not empty
        for query in queries:
            self.assertEqual(query._fields, fields)
            self.assertIsNotNone(query.termo)
