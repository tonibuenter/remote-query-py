# import unittest
# import logging
# import string
from unittest import TestCase
import remote_query as rQ
from remote_query import StatementNode

import test_central
        
        
class Test_While(TestCase):

    def setUp(self):
        test_central.TestCentral.init()

    def test_command_while(self):
        
        serviceId = "Test.Command.while"
        
        #
        # COMMAND NODE
        #
        
        se = rQ.ServiceRepositoryHolder.get(serviceId)
        self.assertIsNotNone(se)
        cb = rQ.prepareCommandBlock(se)
 
        cbExpected = StatementNode("serviceRoot").append(
                #
                StatementNode("sql"),
                #
                StatementNode("sql"),
                #
                StatementNode("sql"),
                #
                StatementNode("set"),
                #
                StatementNode("while").append(
                        #
                        StatementNode("sql"),
                        #
                        StatementNode("parameters"),
                        #
                        StatementNode("end")),
                #
                StatementNode("sql"));

        rQ.assertStatementNodeEquals(cbExpected, cb)
 
        #
        # REQUEST RUN
        #
 
        request = rQ.Request().setServiceId(serviceId) 
        r = request.run()
        self.assertTrue(len(r) == 0)
