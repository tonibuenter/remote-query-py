import logging
import remote_query as rQ

sqlfileNames = [ "init_01_bootstrap.sql", "address.sql", "address_testdata.sql", "html-text.sql" ]
rqSqlfileNames = ["init_02_commands.rq.sql", "init_02over_commands-mysql.rq.sql", "init_03_includes.rq.sql", "init_10_system_services.rq.sql", "address.rq.sql", "html-text.rq.sql"]


class TestCentral:

    logger = logging.getLogger('TestCentral')

    dataSource = None

    @staticmethod
    def init():

        if TestCentral.dataSource :
            TestCentral.logger.info("TestCentral already initialized, will do nothing...")
            return

        #
        # 1. Database : Create a temporary directory for a Appache Derby DB
        # with with embedded driver (not possible with mysql, so we skip this)
        #

        user = "jground"
        password = "1-2-34AB"

        #
        # 2. DataSource : Create a data source
        #

        TestCentral.dataSource = rQ.DataSource(user=user , password=password)
        connection = TestCentral.dataSource.getConnection()

        #
        # 3. DB Objects : Create schema and tables, insert bootstrap service
        # entry
        #
        sqlDir = "/Users/tonibuenter/_proj/remote-query/src/test/java/org/remotequery/tests/"
        for fileName in sqlfileNames:
            print("open file", fileName)
            with open(sqlDir + fileName, 'r', encoding="utf-8") as f:
                text = f.read()
            rQ.processSqlText(connection, text, fileName)

        #
        # 4. Initialize RemoteQuery : Register data source, create and register
        # an sql service
        # repository with the service table JGROUND.T_RQ_SERVICE
        #

        TestCentral.logger.info("Register default data source...")
        rQ.DataSources.register(TestCentral.dataSource)

        rQ.serviceTable = "JGROUND.T_RQ_SERVICE"
        TestCentral.logger.info("Register default ...")
        rQ.ServiceRepositoryHolder = rQ.ServiceRepositorySql(dataSource=TestCentral.dataSource)

        #
        # 5. Load RQ Services : Read application's service definitions from
        # rq.sql files
        #

        for fileName in rqSqlfileNames:
            with open(sqlDir + fileName, 'r', encoding="utf-8") as f:
                text = f.read()
            rQ.processRqSqlText(text, fileName)

        TestCentral.dataSource.returnConnection(connection)
