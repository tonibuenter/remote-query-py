
from unittest import TestCase

import remote_query as rQ

# from docutils.parsers.rst.languages.af import roles


def uuid_query(_request, **_kv):
    result = rQ.Result(header=['uuid'], table=[["1234"]])
    return result


rQ.Pythons['UuidQuery'] = uuid_query        

import test_central
        
class Test_PythonCommand(TestCase):
       
    def setUp(self):
        test_central.TestCentral.init()

    def test_uuid_exist(self):
        serviceId = "UUID.create"
        se = rQ.ServiceRepositoryHolder.get(serviceId)
        if "python" not in se.statements:
            self.fail()
        else:
            print(serviceId, "exists!")

    def test_python(self):
        result = rQ.Request(serviceId="UUID.create", roles=set(["SYSTEM"])).run()
        
        self.assertEqual("1234", result.table[0][0])
