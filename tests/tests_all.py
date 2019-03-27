import logging

from unittest import TestCase
import unittest

import remote_query as rQ

from test_central import TestCentral


class UtilityTests(TestCase):

    def test_to_camel_case(self):
        r = rQ.to_camel_case("hello_world")
        self.assertEqual("helloWorld", r)
        r = rQ.to_camel_case("HELLO_wORLd")
        self.assertEqual("helloWorld", r)

    def test_tokenize1(self):
        r = rQ.tokenize("Hello")
        self.assertEqual(["Hello"], r)

    def test_tokenize2(self):
        r = rQ.tokenize("Hello World", delimiter=" ")
        self.assertEqual(["Hello", "World"], r)

    def test_tokenize4(self):
        r = rQ.tokenize(" Hello World this\ is a test ", delimiter=" ", keepEmpty=False)
        self.assertEqual(["Hello", "World", "this is", "a", "test"], r)
       
    def test_tokenize5(self):
        r = rQ.tokenize(" Hello World this\ is a test ", delimiter=" ", keepEmpty=True)
        self.assertEqual(["", "Hello", "World", "this is", "a", "test", ""], r)
       
    def test_tokenize6(self):
        r = rQ.tokenize(" Hello World this\ is a test   ", delimiter=" ", keepEmpty=True)
        self.assertEqual(["", "Hello", "World", "this is", "a", "test", "", "", ""], r)
       
    def test_tokenize7(self):
        r = rQ.tokenize(" Hello $ World $ this\$  is a $ test   ", keepEmpty=False, strip=True, delimiter="$")
        self.assertEqual(["Hello", "World", "this$  is a", "test"], r)

    def test_split(self):
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])
        # check that s.split fails when the separator is not a string
        with self.assertRaises(TypeError):
            s.split(2)

            
class Utility2Tests(TestCase):

    def test_to_camel_case(self):
        r = rQ.to_camel_case("hello_world")
        self.assertEqual("helloWorld", r)
        r = rQ.to_camel_case("HELLO_wORLd")
        self.assertEqual("helloWorld", r)
        
    def test_convert_query(self):
        
        sql_in = "select * from table where USER = :name and Name = 'toni'"
        sql_exp = "select * from table where USER = %s and Name = 'toni'"
        
        qap = rQ.QueryAndParams(sql_in, {})
        qap.convertQuery()
        
        self.assertEqual(sql_exp, qap.qm_query)
        self.assertEqual(['name'], qap.param_list)
        
        sql_in = "select * from table where USER_ID = :name_id and Name = 'toni'"
        sql_exp = "select * from table where USER_ID = %s and Name = 'toni'"
        
        qap = rQ.QueryAndParams(sql_in, {})
        qap.convertQuery()

        
        
        self.assertEqual(sql_exp, qap.qm_query)
        self.assertEqual(['name_id'], qap.param_list)

    def test_parseStatement(self):
        statement0 = "set  a = b"
        cmd, parameter, statement = rQ.parse_statement(statement0)
        self.assertEqual("set", cmd)
        self.assertEqual("a = b", parameter)
        self.assertEqual(statement0, statement)
        
        statement0 = "set-if-empty \n c=b"
        cmd, parameter, statement = rQ.parse_statement(statement0)
        self.assertEqual("set-if-empty", cmd)
        self.assertEqual("c=b", parameter)
        self.assertEqual(statement0, statement)
        
        statement0 = "parameters c=b"
        cmd, parameter, statement = rQ.parse_statement(statement0)
        self.assertEqual("parameters", cmd)
        self.assertEqual("c=b", parameter)
        self.assertEqual(statement0, statement)

        
class ServiceRepositoryTests(TestCase):

    def test_ServiceRepositoryHolder(self):
        TestCentral.init()
        testService = "RQService.save"
        testRoles = set(["SYSTEM", "APP_ADMIN"])
        se = rQ.ServiceRepositoryHolder.get(testService)
        self.assertTrue(se.serviceId == testService)
        self.assertEqual(se.roles, testRoles)
        
        
class ResolveIncludesTests(TestCase):

    def test_include1(self):
        TestCentral.init()
        se0 = rQ.ServiceRepositoryHolder.get("Test.Include.includer")
        se1 = rQ.ServiceRepositoryHolder.get("Test.Include.result")
        self.assertTrue(se0)
        self.assertTrue(se1)
        statementList = rQ.resolveIncludes(se0.statements)
        tmp = ";".join(statementList)
        
        self.assertEqual(rQ.removeWS(tmp), rQ.removeWS(se1.statements))

# import test_python_command

class Test_Misc(TestCase):

    def test_set(self):
        TestCentral.init()
        serviceId = "Test.Command.set"
        se = rQ.ServiceRepositoryHolder.get(serviceId)
        self.assertIsNotNone(se)
        request = rQ.Request().setServiceId(serviceId) 
        request.run()
        self.assertTrue("hello", request.get("name"))

    def test_set2(self):
        TestCentral.init()        
        serviceId = "Test.Command.copy"
        se = rQ.ServiceRepositoryHolder.get(serviceId)
        self.assertIsNotNone(se)
        request = rQ.Request().setServiceId(serviceId) 
        request.run()
        self.assertEqual("hello", request.get("name"))
        self.assertEqual("hello", request.get("name1"))
        self.assertEqual("hello2", request.get("name2"))
        
    def test_command_backslash(self):
        TestCentral.init()        
        serviceId = "Test.Command.backslash"
        se = rQ.ServiceRepositoryHolder.get(serviceId)
        self.assertIsNotNone(se)
        
        request = rQ.Request()
        request.setServiceId(serviceId) 
        request.run()
        self.assertEqual("ok", request.get("semicolon"))
        
    def test_command_example(self):
        TestCentral.init()                
        serviceId = "Test.Command.example"
        se = rQ.ServiceRepositoryHolder.get(serviceId)
        self.assertIsNotNone(se)
        
        request = rQ.Request().addRole("APP_ADMIN")
        request.setServiceId(serviceId) 
        r = request.run()
        self.assertEqual("world", r.table[0][0])
        
    def test_command_serviceid(self):
        
        TestCentral.init()        
        serviceId = "Test.Command.serviceid"
        se = rQ.ServiceRepositoryHolder.get(serviceId)
        self.assertIsNotNone(se)
        
        request = rQ.Request().addRole("APP_ADMIN")
        request.setServiceId(serviceId) 
        r = request.run()
        self.assertEqual("world", r.table[0][0])
        
    def test_insert_html_text(self):
        TestCentral.init()        
        texttext = "<html>hallo</html>"
        serviceId = "HtmlText.save"
        se = rQ.ServiceRepositoryHolder.get(serviceId)
        self.assertIsNotNone(se)
        
        r = rQ.Request().setServiceId(serviceId).put("htmlTextId", "test_01").put("htmlText", texttext).addRole("APP_USER").run()
        self.assertEqual(1, r.rowsAffected)
        
    def test_set_array_parameters(self):
        TestCentral.init()        
        request = rQ.Request()
        request.setServiceId('Test.Command.arrayParameter') 
        request.run()
        self.assertEqual('New York,Paris,London,Peking', request.get('names'))
        self.assertEqual('New York,Paris,London,Peking', request.get('namesCopy'))
        self.assertEqual('', request.get('namesCopy2'))
         
    def test_array_parameters(self):
        TestCentral.init()        
        request = rQ.Request()
        request.setServiceId('Address.selectWithNamesArray') 
        request.addRole('ADDRESS_READER')
        request.put('names', 'Anna,Ralf,Sara')
        
        
        result = request.run()
         
         
        _list = result.toList()
        
        self.assertEqual(3, len(_list))


# if __name__ == '__main__':
#     
#     rQ.sqlLogger.setLevel(logging.DEBUG)
#     rQ.logger.setLevel(logging.DEBUG)
#     rQ.sqlLogger.debug("TEST DEBUG")
#     rQ.logger.debug("TEST DEBUG")
    
#     TestCentral.init()
#     
#     unittest.main()
