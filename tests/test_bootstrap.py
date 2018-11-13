
from unittest import TestCase
import remote_query as rQ
from remote_query import StatementNode
from test_central import TestCentral
        
        
class Test_Bootstrap(TestCase):

    def test_service_repository_is_ok(self):
        TestCentral.init()
        se = rQ.ServiceRepositoryHolder.get("RQService.select")
        self.assertIsNotNone(se)
        self.assertIsNotNone(se.serviceId)

    def test_sql_serviceId_1(self):
        TestCentral.init()
        se = rQ.ServiceRepositoryHolder.get("RQService.delete")
        self.assertIsNotNone(se)
        self.assertIsNotNone(se.serviceId)
        self.assertIsNotNone(se.roles)
        self.assertTrue(len(se.roles) > 0)
        self.assertTrue("SYSTEM" in se.roles)
        self.assertFalse("USER" in se.roles)

    def test_assert_statement_node(self):
        a1 = StatementNode("serviceRoot").append(
                StatementNode("parameters"),
                StatementNode("if").append(
                    StatementNode("sql"),
                    StatementNode("sql"),
                    StatementNode("else"),
                    StatementNode("sql"),
                    StatementNode("end")
                )
            )
        
        a2 = StatementNode("serviceRoot").append(
        StatementNode("parameters"),
        StatementNode("if").append(
            StatementNode("sql"), StatementNode("sql"), StatementNode("else"), StatementNode("sql"),
            StatementNode("end")
            )
        )
        self.assertTrue(rQ.assertStatementNodeEquals(a1, a2))
        
    def test_parse(self):
        
        def _parse_(statementRaw, cmd0, parameter0, statement0):
            cmd, parameter, statement = rQ.parse_statement(statementRaw)
            if not cmd:
                self.assertEqual(cmd0, cmd)
                return
                   
            self.assertEqual(cmd0, cmd)
            self.assertEqual(parameter0, parameter)
            self.assertEqual(statement0, statement)
        
        _parse_("  ", None, None, None)
        _parse_(" set a = b ", "set", "a = b", "set a = b")
        _parse_("set a = b", "set", "a = b", "set a = b")
        _parse_("set   a = b", "set", "a = b", "set   a = b")
        _parse_(" set-if-empty a=b ", "set-if-empty", "a=b", "set-if-empty a=b")
        _parse_("serviceId This_and that", "serviceId", "This_and that", "serviceId This_and that")
        _parse_("Select * from ldsfkds", "sql", "Select * from ldsfkds", "Select * from ldsfkds")
        _parse_("settti: * from ldsfkds", "sql", "settti: * from ldsfkds", "settti: * from ldsfkds")
        _parse_(" if   parameter 123 ", "if", "parameter 123", "if   parameter 123")
        _parse_("if para3", "if", "para3", "if para3")
        _parse_("then", "then", "", "then")

        t = "set   a b'\"";
        _parse_(t, "set", "a b'\"", t.strip());
            
    def test_tokenize(self):
        
        def testtok(list_):
            s = rQ.joinTokens(list_, delimiter=',')
            a = rQ.tokenize(s, delimiter=',')
            self.assertEqual(list_, a)
        
        testtok(["a", "b", "c,,,,,,,,,,,,b", "set-0:c=b" ])
        testtok(["a"])
        testtok([])
        
        self.assertEqual(2, len(rQ.tokenize("set-0:a=b", delimiter=":")))
        self.assertEqual(2, len(rQ.tokenize(rQ.tokenize("set-0:a=b", ':', '\\')[1], '=', '\\')))
        self.assertEqual(3, len(rQ.tokenize("this \\, is, a,test", delimiter=",")))
        self.assertEqual(4, len(rQ.tokenize("this , is, a,test", delimiter=",")))
        self.assertEqual("this,is", rQ.joinTokens(["this", "is"]))
        self.assertEqual("t\\,his,is", rQ.joinTokens(["t,his", "is"]))
        
