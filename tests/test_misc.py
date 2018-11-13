from unittest import TestCase
import remote_query as rQ

from  test_central import TestCentral
        
        
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
        request = rQ.Request()
        request.setServiceId('Test.Command.arrayParameter') 
        request.run()
        self.assertEqual('New York,Paris,London,Peking', request.get('names'))
        self.assertEqual('New York,Paris,London,Peking', request.get('namesCopy'))
        self.assertEqual('', request.get('namesCopy2'))
         
    def test_array_parameters(self):
        request = rQ.Request()
        request.setServiceId('Address.selectWithNamesArray') 
        request.addRole('ADDRESS_READER')
        request.put('names', 'Anna,Ralf,Sara')
        result = request.run()
         
         
        _list = result.toList()
        self.assertEqual(3, len(_list))
        