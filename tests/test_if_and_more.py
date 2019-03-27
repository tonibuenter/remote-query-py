
from unittest import TestCase
import remote_query as rQ
from remote_query import StatementNode

import test_central
        
        
class Test_Commands_if_and_more(TestCase):
        
    def setUp(self):
        test_central.TestCentral.init()

    def test_command_if(self):
        
        #
        # COMMAND NODE
        #
 
        se = rQ.ServiceRepositoryHolder.get("Test.Command.if")
        self.assertIsNotNone(se)
        snActual = rQ.prepareCommandBlock(se)
 
        snExpected = StatementNode("serviceRoot").append(StatementNode("parameters"),
                        StatementNode("if").append(
                            StatementNode("sql"), StatementNode("sql"), StatementNode("else"), StatementNode("sql"),
                            StatementNode("end")
                        ))
 
        rQ.assertStatementNodeEquals(snExpected, snActual)
 
        #
        # REQUEST RUN
        #
 
        request = rQ.Request().setServiceId("Test.Command.if").put("name", "hello") 
        r = request.run()
        self.assertTrue(len(r) > 0)
        m = r.toList()
        self.assertEqual("true", m[0]["value"])
        request.put("name", "blabla")
        r = request.run()
        self.assertTrue(len(r) > 0)
        self.assertEqual("false", r.toList()[0]["value"]);


    def test_command_if_empty(self):
        
        #
        # COMMAND NODE
        #
 
        se = rQ.ServiceRepositoryHolder.get("Test.Command.if-empty")
        self.assertIsNotNone(se)
        
 
        #
        # REQUEST RUN
        #
 
        request = rQ.Request().setServiceId("Test.Command.if-empty").put("name", "does not exist") 
        request.run()
        self.assertEqual("true", request.get('emptyVisited'));
        self.assertEqual("false", request.get('else'));
        
    def test_command_if_else_only(self):
        
        serviceId = "Test.Command.if_elseOnly"
    
        #
        # COMMAND NODE
        #
    
        se = rQ.ServiceRepositoryHolder.get(serviceId)
        self.assertIsNotNone(se)
        cb = rQ.prepareCommandBlock(se)
    
        cbExpected = StatementNode("serviceRoot").append(
            #
                 StatementNode("put"),
                  StatementNode("if").append(
                    #
                         StatementNode("else"),
                    #
                         StatementNode("put"),
                    #
                         StatementNode("end")
            #
            ))
    
        rQ.assertStatementNodeEquals(cbExpected, cb)
    
        #
        # REQUEST RUN
        #
    
        request = rQ.Request().setServiceId(serviceId)
    
        request.run()
        self.assertEqual("true", request.get("elseValue"))
    
        request.put("condition1", "hello")  
    
        request.run()   
    
        self.assertEqual("not reached else", request.get("elseValue"))

    def test_command_switch(self):
        
        serviceId = "Test.Command.switch"
        
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
                  StatementNode("parameters"),
                #
                  StatementNode("switch").append(
                        #
                        StatementNode("case"),
                        #
                        StatementNode("case"),
                        #
                        StatementNode("case"),
                        #
                        StatementNode("sql"),
                        #
                        StatementNode("break"),
                        #
                        StatementNode("case"),
                        #
                        StatementNode("sql"),
                        #
                        StatementNode("break"),
                        #
                        StatementNode("case"),
                        #
                        StatementNode("sql"),
                        #
                        StatementNode("break"),
                        #
                        StatementNode("case"),
                        #
                        StatementNode("sql"),
                        #
                        StatementNode("end")),
                #
                StatementNode("sql"),
                #
                StatementNode("parameters"),
                #
                StatementNode("sql"));
                
        rQ.assertStatementNodeEquals(cbExpected, cb)
    
        #
        # REQUEST RUN
        #
    
        request = rQ.Request().setServiceId(serviceId)
        request.run()
        total1 = request.get("total1")
        self.assertIsNotNone(total1)
        self.assertEqual("2", total1)
     
    def    test_command_switch_empty(self):
        
        serviceId = "Test.Command.switch_empty"
        #
        # COMMAND NODE
        #
        
        se = rQ.ServiceRepositoryHolder.get(serviceId)
        self.assertIsNotNone(se)
        cb = rQ.prepareCommandBlock(se)

        cbExpected = StatementNode("serviceRoot").append(
                #
                StatementNode("set"),
                #
                StatementNode("sql"),
                #
                StatementNode("set"),
                #
                StatementNode("set"),
                #
                StatementNode("switch").append(
                        #
                        StatementNode("case"),
                        #
                        StatementNode("case"),
                        #
                        StatementNode("case"),
                        #
                        StatementNode("sql"),
                        #
                        StatementNode("break"),
                        #
                        StatementNode("case"),
                        #
                        StatementNode("sql"),
                        #
                        StatementNode("break"),
                        #
                        StatementNode("case"),
                        #
                        StatementNode("sql"),
                        #
                        StatementNode("end")),
                #
                StatementNode("sql"),
                #
                StatementNode("parameters"),
                #
                StatementNode("sql"))
        
        rQ.assertStatementNodeEquals(cbExpected, cb)

        #
        # REQUEST RUN
        #
        
        request = rQ.Request().setServiceId(serviceId)
        request.run()
        total1 = request.get("total1")
        self.assertIsNotNone(total1)
        self.assertEqual("2", total1)

    def test_command_switch_default(self):
        
        serviceId = "Test.Command.switch_default"
        
        #
        # COMMAND NODE
        #
        
        se = rQ.ServiceRepositoryHolder.get(serviceId)
        self.assertIsNotNone(se)
        cb = rQ.prepareCommandBlock(se)
 
        cbExpected = StatementNode("serviceRoot").append(
                #
                StatementNode("set"),
                #
                StatementNode("sql"),
                #
                StatementNode("set"),
                #
                StatementNode("set"),
                #
                StatementNode("switch").append(
                        #
                        StatementNode("case"),
                        #
                        StatementNode("case"),
                        #
                        StatementNode("case"),
                        #
                        StatementNode("sql"),
                        #
                        StatementNode("break"),
                        #
                        StatementNode("default"),
                        #
                        StatementNode("sql"),
                        #
                        StatementNode("break"),
                        #
                        StatementNode("case"),
                        #
                        StatementNode("sql"),
                        #
                        StatementNode("end")),
                #
                StatementNode("sql"),
                #
                StatementNode("parameters"),
                #
                StatementNode("sql"));
 
        rQ.assertStatementNodeEquals(cbExpected, cb)

        #
        # REQUEST RUN
        #
        
        request = rQ.Request().setServiceId(serviceId)
        request.run()
        total1 = request.get("total1")
        self.assertIsNotNone(total1)
        self.assertEqual("1", total1)
        
    def test_command_foreach(self):
        
        serviceId = "Test.Command.foreach"
        
        #
        # COMMAND NODE
        #
        
        se = rQ.ServiceRepositoryHolder.get(serviceId)
        self.assertIsNotNone(se)
        cb = rQ.prepareCommandBlock(se)

        cbExpected = StatementNode("serviceRoot").append(
                #
                StatementNode("parameters"),
                #
                StatementNode("foreach").append(
                        #
                        StatementNode("sql"),
                        #
                        StatementNode("end")),
                #
                StatementNode("sql"),
                #
                StatementNode("parameters"));
 
        rQ.assertStatementNodeEquals(cbExpected, cb)

        #
        # REQUEST RUN
        #
        
        request = rQ.Request().setServiceId(serviceId)
        request.run()
        total1 = request.get("total1")
        total2 = request.get("total2")
        self.assertIsNotNone(total1)
        self.assertIsNotNone(total2)

