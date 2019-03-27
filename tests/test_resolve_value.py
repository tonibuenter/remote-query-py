# import unittest
# import string
from unittest import TestCase
import remote_query as rQ
import test_central
        
        
class Test_Resolve_Value(TestCase):
    
    def setUp(self):
        test_central.TestCentral.init()

    def test1(self):
        request = rQ.Request(parameters={'firstName':'Johansson','zero':''})
        request.put("firstName", "Johansson");
        request.put("zero", "");
        request.put("none", None);
        self.assertEqual("Johansson", rQ.resolve_value(":firstName", request));
        self.assertEqual("Johansson", rQ.resolve_value("  :firstName  ", request));
        self.assertEqual("firstName", rQ.resolve_value("firstName", request));
        self.assertEqual(":lastName", rQ.resolve_value("':lastName'", request));
        #
        self.assertEqual("", rQ.resolve_value(":zero", request));
        self.assertEqual("", rQ.resolve_value("  :zero  ", request));
        self.assertEqual("zero", rQ.resolve_value("zero", request));
        #
        self.assertEqual("", rQ.resolve_value(":none", request));
        self.assertEqual("", rQ.resolve_value("  :none  ", request));
        self.assertEqual("none", rQ.resolve_value("none", request));
        #
        self.assertEqual("'", rQ.resolve_value("'", request));
        #
        self.assertEqual("", rQ.resolve_value(None, request));
        self.assertEqual("", rQ.resolve_value(":lastName", request));
        #
        self.assertEqual("A B", rQ.resolve_value("   'A B' ", request));
        self.assertEqual("A B", rQ.resolve_value("'A B'", request));
        self.assertEqual("A B", rQ.resolve_value("A B", request));
        self.assertEqual("A B", rQ.resolve_value(" A B ", request));
        self.assertEqual(":firstName", rQ.resolve_value("':firstName'", request));
        #
