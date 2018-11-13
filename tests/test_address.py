from unittest import TestCase
import remote_query as rQ
from test_central import TestCentral
        
        
class Test_Address(TestCase):

    def test_address_filter(self):
        TestCentral.init()

        request = rQ.Request(
                serviceId="Address.search",
                roles=set(["ADDRESS_READER"]),
                parameters={"nameFilter": "Jo%"})
        result = request.run()
        # convert to a POJO
        _list = result.toList()
        self.assertEqual(2, len(_list))
