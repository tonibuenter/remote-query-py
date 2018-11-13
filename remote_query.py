import copy
import json
import logging
import threading
import time
import traceback
from mysql.connector import DatabaseError
from mysql.connector import FieldType
from mysql.connector.pooling import MySQLConnectionPool
from _collections import defaultdict

#   
# ServiceRepositoryHolder
#
ServiceRepositoryHolder = None

# TODO how can we do the default initialization just once (no overwrite of later settings?)
#   
# saveServiceId
#
saveServiceId = 'RQService.save'

#   
# TLD (thread local data)
#
TLD = threading.local()


#   
# M (message concatinator)
#
def M(*args):
    m = ''
    for e in args:
        m += str(e) + ' '
    return m   


class DataSource:

    def __init__(self, host='127.0.0.1', port='3306', user='root',
                 password='', database='jground', _pool_name='remote_query',
                 _pool_size=10):
        
        self.pool = MySQLConnectionPool(pool_size=_pool_size,
                                        host=host,
                                        port=port,
                                        user=user,
                                        password=password,
                                        database=database,
                                        pool_reset_session=True,
                                        autocommit=True, charset='utf8mb4')
    
    def getConnection(self):
        return self.pool.get_connection()
    
    def returnConnection(self, con):
        con.close()


# 
# Init logging
# 


logger = logging.getLogger('remote_query.log')
sqlLogger = logging.getLogger('remote_query.sql')

ENCODING = 'UTF-8'

MAX_RECURSION = 40
MAX_INCLUDES = 100
MAX_WHILE = 10000

DEFAULT_DEL = ','
DEFAULT_ESC = '\\'

ANONYMOUS = 'ANONYMOUS'

STATEMENT_DELIMITER = ';'
STATEMENT_ESCAPE = '\\'
    
# 'DEFAULT_DATASOURCE' is the default data source name.
    
DEFAULT_DATASOURCE = 'DEFAULT_DATASOURCE'

COLUMN_STATEMENTS = 'STATEMENTS'
COLUMN_ROLES = 'ROLES'
COLUMN_SERVICE_ID = 'SERVICE_ID'
COLUNM_DATASOURCE = 'DATASOURCE'


#   
# ServiceEntry
#
class ServiceEntry:

    def __init__(self, serviceId='', statements='', roles=set(), datasource=DEFAULT_DATASOURCE):
        self.serviceId = serviceId
        self.statements = statements
        self.roles = roles
        self.datasource = datasource


class Commands:
    
    StartBlock = set()
    StartBlock.add('if')
    StartBlock.add('switch')
    StartBlock.add('while')
    StartBlock.add('foreach')
    
    EndBLock = set()
    EndBLock.add('fi')
    EndBLock.add('done')
    EndBLock.add('end')
    Registry = dict()
    
    @staticmethod
    def isCmd(cmd):
        return cmd in Commands.StartBlock or cmd in Commands.EndBLock or cmd in Commands.Registry


class SqlValueChar: 
    pass


class SqlValueClob: 
    pass


SqlValueMap = dict()

SqlValueMap['VARCHAR'] = SqlValueChar()
SqlValueMap['CHAR'] = SqlValueChar()
SqlValueMap['CLOB'] = SqlValueClob()

Pythons = {}


class DataSources:
    
    dss = dict()
    
    @staticmethod
    def get(name=None):
        return DataSources.dss[name if name else DEFAULT_DATASOURCE]
 
    @staticmethod    
    def register(ds, name=None):
        DataSources.dss[name if name else DEFAULT_DATASOURCE] = ds


class ServiceRootCommand: 
    pass


def buildCommandBlockTree(root, statementList, pointer):

    while pointer < len(statementList):

        cmd, parameter, statement = parse_statement(statementList[pointer])
        pointer = pointer + 1

        if not cmd:
            continue

        statementNode = StatementNode(cmd, parameter, statement)
        root.children.append(statementNode)

        if cmd in Commands.EndBLock:
            return pointer

        if cmd in Commands.StartBlock:
            pointer = buildCommandBlockTree(statementNode, statementList, pointer)
    return pointer


def resolveIncludes(statements, recursionCounter={}):
    
    statementList = tokenize(statements.strip(), delimiter=STATEMENT_DELIMITER, escape=STATEMENT_ESCAPE, keepEmpty=False)
    resolvedList = []    
    for stmt in statementList:
        stmt = stmt.strip()
        _inc = 'include'
        if stmt[:len(_inc)] == _inc:
            serviceId = ''
            try:
                _0, serviceId, _2 = parse_statement(stmt)
                se = ServiceRepositoryHolder.get(serviceId)                
                counter = recursionCounter.get(se.serviceId, 0) #recursionCounter[se.serviceId] if se.serviceId in recursionCounter else 0
                counter += 1
                if counter < MAX_INCLUDES:
                    recursionCounter[se.serviceId] = counter
                    resolvedList2 = resolveIncludes(se.statements, recursionCounter)
                    for s in resolvedList2:
                        resolvedList.append(s)                  
                else:
                    logger.error(M('include command overflow:', se.serviceId))
                
            except Exception as e:
                logger.error(e)
                resolvedList.append(M('systemMessage:include-of-error-serviceId:', se.serviceId))
        else:
            resolvedList.append(stmt)
    return resolvedList


#
#  prepareCommandBlock
#
def prepareCommandBlock(se) :
    serviceId, statements = se.serviceId, se.statements
    statementList = resolveIncludes(statements, {})
    statementNode = StatementNode('serviceRoot', serviceId, '')
    buildCommandBlockTree(statementNode, statementList, 0)
    return statementNode

#
# run
# 
# def run_result(serviceId, parameters=None, roles=None, userId=None):
#     request = Request(serviceId, parameters=parameters, roles=roles, userId=userId)
#     runner = Runner()
#     return runner.run(request)


#
# processCommandBlock
#
def processCommandBlock(statementNode, request, currentResult, serviceEntry):
    log = ProcessLog.Current()
    try :
        log.incrRecursion()
        if log.recursion > MAX_RECURSION:
            msg = M('Recursion limit reached with:', MAX_RECURSION, '. Stop processing.')
            logger.error(msg)
            return Result(exception=msg)        
        
        fun = Commands.Registry.get(statementNode.cmd)
        if fun:
            return fun(request, currentResult=currentResult, statementNode=statementNode, serviceEntry=serviceEntry)
        else:
            logger.error(M('unknown command:' , statementNode.cmd , 'in statement:' , statementNode.statement))
        
    except Exception as e:
        logger.error(M('statement:' , statementNode.statement , '->', e))
        print(traceback.format_exc())
    finally:
        log.decrRecursion()


#
# processCommand
#
def processCommand(commandString, request, currentResult, serviceEntry):
    cmd, parameter, statement = parse_statement(commandString)
    statementNode = StatementNode(cmd, parameter, statement)
    return processCommandBlock(statementNode, request, currentResult, serviceEntry)

    
#
# processSql_serviceEntry
#
def processSql_serviceEntry(sql, request, serviceEntry=ServiceEntry()):   
    ds = DataSources.get(serviceEntry.datasource)
    if not ds:
        msg = M('missing datasource name:', serviceEntry.datasource) 
        logger.error(msg)
        result = Result(exception=msg)
        return result
    return processSql_ds(ds, sql, request, serviceEntry.serviceId)

    
#
# processSql_ds
#
def processSql_ds(ds, sql, request, name='no_name'):
    con = None
    r = None
    try:
        con = ds.getConnection()
        r = processSql_con(con, sql, parameters=request.parameters, name=name)
    except Exception as e:
        r = Result(exception=M(e))
        logger.error(e)
    finally:
        ds.returnConnection(con)
    return r

    
#
# processSql_con
#
def processSql_con(con, sql, parameters={}, maxRows=2000, name='no_name'):

    pLog = ProcessLog.Current()

    pLog.system('sql before conversion:', sql)
    sqlLogger.debug('start sql **************************************')
    sqlLogger.debug(M(name, sql))
    qap = QueryAndParams(sql, parameters)
    qap.convertQuery()
    sql = qap.qm_query
    names = qap.param_list
    parameters = qap.req_params
    #
    # PREPARE SERVICE_STMT
    #

    sql_params = []

    for n in names:
        if n in parameters:
            v = parameters[n]
            sql_params.append(v);
            sqlLogger.debug(M('sql-parameter:', n, ':' , v))
        else:   
            sqlLogger.info(M(name, 'no value provided for parameter:' , n , 'will use empty string'))
            sql_params.append('')

    #
    # FINALIZE SERVICE_STMT
    #
    result = Result()
    result.name = name
    result.rowsAffected = -1
    try:       
        cursor = con.cursor(dictionary=True)
        cursor.execute(sql, sql_params)
        if (cursor.with_rows):
            rows = cursor.fetchmany(size=(maxRows + 1))
            sqlLogger.debug(M(name, 'sql-rows-found:', len(rows)))
            result.header = []
            result.types = []
            _header = []
            for desc in cursor.description:
                _header.append(desc[0])
                result.header.append(to_camel_case(desc[0]))
                result.types.append(FieldType.get_info(desc[1]))
                
            result.table = []
            for row in rows:
                r = []
                for head in _header:
                    v = row[head]
                    r.append('' if v is None else str(v))
                result.table.append(r) 
                if len(result.table) >= maxRows:
                    break
            result.totalCount = len(rows)
            
        else:
            result.rowsAffected = cursor.rowcount;
            sqlLogger.debug(M('sql-rows-affected:', result.rowsAffected))
        
    except DatabaseError as e:
        msg = M(name, 'parameters:', parameters, '->', e)
        pLog.warn(msg)
        sqlLogger.warning(msg)
  
    except Exception as e:
        msg = M(name, 'parameters:', parameters, '->', e)
        pLog.error(msg)
        logger.error(msg)
        
    finally:
        con.commit()
        closeQuietly(cursor)
    return result

#
# run_request
#


def run_request(request) :
    result = Result()
    log = ProcessLog.Current()

    log.incrRecursion(request.serviceId)
    try:
        se = ServiceRepositoryHolder.get(request.serviceId)
        
        if not se or not se.serviceId:
            log.error('No ServiceEntry found for ', request.serviceId)
            logger.error('No ServiceEntry found for %s' % request.serviceId);
            return result
        
        #
        # CHECK ACCESS
        #
        
        if se.roles and len(se.roles.intersection(request.roles)) == 0:
            msg = M('No access to', se.serviceId, 'for', request.userId, '(service roles:', se.roles, 'request roles:' , request.roles, ')')
            log.warn(msg)
            log.statusCode = '403'
            result.exception = msg
            logger.warn(msg)
            return result
        logger.info(M('Access to:', se.serviceId, 'for:', request.userId, 'is granted'))

        #
        # START PROCESSING STATEMENTS
        #

        logger.info(M('Service found for userId:', request.userId, 'is:', se.serviceId))

        statementNode = prepareCommandBlock(se)

        result = processCommandBlock(statementNode, request, result, se)

    except Exception as e:
        logger.error(e)
        log.error(e)
    finally:
        log.decrRecursion(request.serviceId)
    
    if  result is not None:  
        result.userId = request.userId
    
    return result


#
#
# BuildIns ...
#
#
def multi_service(request, *_p, **_kv):
    
    mainLog = ProcessLog.Current()
    mainResult = Result(name='MultiResult')
    mainResult.processLog = mainLog
    requestArray = request.get('requestArray')
    
    if not requestArray:
        mainResult.exception = 'Parameter requestArray is empty'
        return mainResult
    
    requestList = json.loads(requestArray)
    mainResult.header = ['json']
    for i, r in enumerate(requestList):
        requestC = copy.deepcopy(request)
        requestC.parameters.pop('requestArray', None)
        
        if r and isinstance(r, dict) and 'serviceId' in r:
            serviceId = r['serviceId']
            if serviceId:
                requestC.serviceId = serviceId
                parameters = defaultdict(lambda:'')
                if 'parameters' in r:
                    parameters.update(r['parameters'])
                parameters.update(requestC.parameters)
                requestC.parameters = parameters     
                resultC = requestC.run()
            else:
                resultC = Result(exception='No serviceId')
            
        else:
            msg = M('Request:', i, ' is not an object (dict):', r)
            resultC = Result(exception=msg)
        mainResult.table.append([resultC.toJson()])     
    return mainResult


Pythons['multi_service'] = multi_service

#   
# ProcessLog
#


class ProcessLog:

    USER_OK_CODE = 10
    USER_WARNING_CODE = 20
    USER_ERROR_CODE = 30
    OK_CODE = 1000
    WARNING_CODE = 2000
    ERROR_CODE = 3000
    SYSTEM_CODE = 4000

    Warning = 'Warning'
    Error = 'Error'
    OK = 'OK'
    System = 'System'
    USER_CODE_MAP = {OK:USER_OK_CODE, Warning:USER_WARNING_CODE, Error:USER_ERROR_CODE}
    
    def __init__(self):
        self.logLines = []
        self.recursion = 0
        
    def incrRecursion(self, _name='no_name'):
        self.recursion += 1

    def decrRecursion(self, _name='no_name'):
        self.recursion -= 1
    
    @staticmethod    
    def Current():
        if not hasattr(TLD, 'processLog'):
            TLD.processLog = ProcessLog()
        return TLD.processLog
    
    def system(self, *args):       
        self.logLines.append({ 'message':M(args), 'state':ProcessLog.System, 'code':ProcessLog.SYSTEM_CODE, 'time':currentTimeMillis() })
        
    def info(self, *args):
        self.logLines.append({ 'message':M(args), 'state':ProcessLog.OK, 'code':ProcessLog.OK_CODE, 'time':currentTimeMillis() })
        logger.info(M(args))

    def warn(self, *args):
        self.logLines.append({ 'message':M(args), 'state':ProcessLog.Warning, 'code':ProcessLog.WARNING_CODE, 'time':currentTimeMillis() })

    def error(self, *args):
        self.logLines.append({ 'message':M(args), 'state':ProcessLog.Error, 'code':ProcessLog.ERROR_CODE, 'time':currentTimeMillis() })
    
       
class LogLine:

    def __init__(self, message, code, state, time):
        self.message = message
        self.code = code
        self.state = state
        self.time = time


#   
# Request
#
class Request:

    def __init__(self, serviceId='', parameters=None, userId=None, roles=None, files=None):
        self.serviceId = serviceId
        self.parameters = parameters if parameters else {}
        self.userId = userId if userId else ANONYMOUS
        self.roles = roles if roles else set()
        self.files = files if files else []
        
    def setServiceId(self, serviceId):
        self.serviceId = serviceId    
        return self

    def put(self, key, value):
        self.parameters[key] = value
        return self

    def get(self, key):
        if key in self.parameters:
            return self.parameters[key]

    def remove(self, key):
        del self.parameters[key]
        return self

    def put_dict(self, map_):
        self.parameters.update(map_)
        return self
    
    def addRoles(self, roles):  
        self.roles.update(roles)
        return self
    
    def addRole(self, role):  
        self.roles.add(role)
        return self

    def removeRole(self, role):
        self.roles.discard(role)
        return self
    
    def run(self):
        return run_request(self)
    
    def run_service(self, serviceId):
        self.serviceId = serviceId
        return self.run()


#   
# Result
#
class Result:

    def __init__(self, header=None, table=None, exception=None, processLog=None, rowsAffected=-1, name='', userId='-'):
        self.name = name
        self.userId = userId
        self.from_ = 0
        self.rowsAffected = rowsAffected
        self.hasMore = False 
        self.header = header if header else []
        self.table = table if table else []
        self.exception = exception
        self.processLog = processLog
        self.totalCount = len(table) if self.table else 0
        
    def __len__(self):
        if self.table:
            return len(self.table)
        else:
            return 0
    
    def toList(self):
        res = []
        if self.header and self.table:
            for row in self.table:
                rrow = dict()
                res.append(rrow)
                for name, value in zip(self.header, row):
                    rrow[name] = value
        return res   
    
    def row(self, index=0):
        res = self.toList()
        if index < len(res):
            return res[index]
        return dict()    
    
    def column(self, column):
        index = self.header.index(column)
        r = [e[index] for e in self.table]
        return r
    
    def single(self):
        if self.table and self.table[0] :
            return self.table[0][0]

    def toJson(self):
        self.size = len(self)
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def results_coalesce(r1, r2):
    return r1 if r1 is not None else r2


serviceTable = ''

serviceQuery = ''


#   
# ServiceRepositorySql
#
class ServiceRepositorySql:

    def __init__(self, dataSource=None):
        self.dataSource = dataSource
        if not dataSource:
            logger.error('No Data Source!')
            return
        if not serviceQuery and not serviceTable:
            logger.error('No Service Query and not Service Table defined (rQ.serviceQuery, rQ.serviceTable)!')
        self.selectQuery = serviceQuery
        if serviceTable:
            self.selectQuery = 'select ' + COLUMN_STATEMENTS + ', ' + COLUMN_ROLES + ', ' + COLUNM_DATASOURCE + ' from ' + serviceTable + ' where ' + COLUMN_SERVICE_ID + ' = %s'
        
    def get(self, serviceId) :
        '''
        Returning a tuple with serviceId, statements, roles (a set), datasource
        '''
        dataSource = DataSources.get()
        con = None
        cursor = None
        try:
            con = dataSource.getConnection()
            cursor = con.cursor(dictionary=True)
            cursor.execute(self.selectQuery, (serviceId,))
            row = cursor.fetchone()
            if row:
                statements = row[COLUMN_STATEMENTS]
                roles = row[COLUMN_ROLES]
                datasource = row[COLUNM_DATASOURCE]
                roles = tokenize(roles, delimiter=DEFAULT_DEL, strip=True, keepEmpty=True)
                roles = set(roles)
                return ServiceEntry(serviceId, statements, roles, datasource)
        except DatabaseError as e:
            sqlLogger.error(e, exc_info=True) 
        except Exception as e:
            logger.error(e, exc_info=True) 
        finally:
            closeQuietly(cursor);
            dataSource.returnConnection(con)


#
# StatementNode
#
class StatementNode:

    def __init__(self, cmd, parameter='', statement=''):
        self.statement = statement
        self.cmd = cmd
        self.parameter = parameter
        self.children = []
        self.pointer = 0
        
    def append(self, *statementNode):
        for statementNode in statementNode:
            self.children.append(statementNode)
        return self;    


#
# sql_command
#
def sql_command(request, currentResult=None, statementNode=None, serviceEntry=ServiceEntry()):
    r = processSql_serviceEntry(statementNode.parameter, request, serviceEntry)
    return results_coalesce(r, currentResult)


Commands.Registry['sql'] = sql_command


def resolve_value(term, request):
    # reference to parameter
    term = '' if term is None else term.strip()
    if len(term) < 2:
        return term
    if term[0] == ':' :
        if term[1:] in request.parameters:
            return request.parameters[term[1:]] or ''
        else:
            return ''
    if term[0] == "'" and term[-1:] == "'":
        return  term[1:-1]
    return term;


#
# set_command
#
def set_command(request, currentResult=None, statementNode=None, serviceEntry=None, overwrite=True):
    
    nv = tokenize(statementNode.parameter, delimiter='=')
    n = nv[0]
    v = nv[1] if len(nv) > 1 else None
    n = n.strip()
    v = resolve_value(v, request)
    requestValue = None
    if n in request.parameters:
        requestValue = request.parameters[n]

    if overwrite or not requestValue:
        request.put(n, v)
    
    return currentResult


Commands.Registry['set'] = set_command
Commands.Registry['put'] = set_command


#
# set_if_empty_command
#
def set_if_empty_command(request, currentResult=None, statementNode=None, serviceEntry=None):    
    return set_command(request, currentResult, statementNode, serviceEntry, overwrite=False)


Commands.Registry['set-if-empty'] = set_if_empty_command
Commands.Registry['put-if-empty'] = set_if_empty_command


#
# copy_command
#
def copy_command(request, currentResult=None, statementNode=None, serviceEntry=None, overwrite=True):
    nv = tokenize(statementNode.parameter, delimiter='=')
    if not nv or not len(nv) == 2:
        logger.warn(
                'Expected parameter-part should be like name1 = name2. But was: ' + statementNode.parameter)
        return currentResult
    targetName = nv[0].strip()
    sourceName = nv[1].strip()

    oldValue = request.get(targetName)
    newValue = request.get(sourceName)

    if overwrite or not oldValue:
        request.put(targetName, newValue)
    
    return currentResult


Commands.Registry['copy'] = copy_command


#
# copy_if_empty_command
#
def copy_if_empty_command(request, currentResult=None, statementNode=None, serviceEntry=None):
    return copy_command(request, currentResult, statementNode, serviceEntry, overwrite=False)


Commands.Registry['copy-if-empty'] = copy_if_empty_command


#
# parameters_command
#
def parameters_command(request, currentResult=None, statementNode=None, serviceEntry=None, overwrite=True):

    cmd, parameter, statement = parse_statement(statementNode.parameter)
    iCb = StatementNode(cmd, parameter, statement)

    r = processCommandBlock(iCb, request, currentResult, serviceEntry)

    if r is not None:            
        parameters = r.row(0)
        for key in r.header:
            if not request.get(key) or overwrite:
                request.put(key, parameters.get(key))
                
                
    return currentResult


Commands.Registry['parameters'] = parameters_command


#
# parameters_if_empty_command
#
def parameters_if_empty_command(request, currentResult=None, statementNode=None, serviceEntry=None):
    return parameters_command(request, currentResult, statementNode, serviceEntry, overwrite=False)


Commands.Registry['parameters-if-empty'] = parameters_if_empty_command


#
# service_id_command
#
def service_id_command(request, statementNode=None, currentResult=None, **kv):
    iRequest = copy.deepcopy(request)
    iRequest.serviceId = statementNode.parameter
    r = run_request(iRequest)
    return results_coalesce(r, currentResult)


Commands.Registry['serviceId'] = service_id_command


#
# python_command
#
def python_command(request, currentResult=None, statementNode='missing_command', serviceEntry=None):
    r = None
    try:
#         cmd = eval(statementNode.parameter)
        cmd = Pythons[statementNode.parameter]
        r = cmd(request, currentResult=currentResult, statementNode=statementNode, serviceEntry=serviceEntry)
    except Exception as e:
        msg = M('Could not process python command:', statementNode.parameter, '->', e)    
        logger.error(msg)
        r = Result(exception=msg)    

    return results_coalesce(r, currentResult)


Commands.Registry['python'] = python_command


#
# service_root_command
#
def service_root_command(request, currentResult=None, statementNode=None, serviceEntry=None):
    for  cbChild in statementNode.children:
        r = processCommandBlock(cbChild, request, currentResult, serviceEntry)
        currentResult = results_coalesce(r, currentResult)
    return currentResult


Commands.Registry['serviceRoot'] = service_root_command


#
# noop_command
#
def noop_command(request, currentResult=None, statementNode=None, serviceEntry=None):
    return currentResult


Commands.Registry['then'] = noop_command
Commands.Registry['else'] = noop_command
Commands.Registry['case'] = noop_command
Commands.Registry['default'] = noop_command
Commands.Registry['break'] = noop_command
Commands.Registry['fi'] = noop_command
Commands.Registry['do'] = noop_command
Commands.Registry['done'] = noop_command
Commands.Registry['end'] = noop_command
Commands.Registry['class'] = noop_command
Commands.Registry['java'] = noop_command
Commands.Registry['include'] = noop_command


#
# if_command
#
def  if_command(request, currentResult=None, statementNode=None, serviceEntry=None):

    isThen = bool(resolve_value(statementNode.parameter, request))

    for cbChild in statementNode.children:
        if 'else' == cbChild.cmd:
            isThen = not isThen
            continue
        
        if isThen:
            r = processCommandBlock(cbChild, request, currentResult, serviceEntry)
            currentResult = results_coalesce(r, currentResult)
            
    return currentResult;


Commands.Registry['if'] = if_command


#
# switch_command
#
def switch_command(request, currentResult=None, statementNode=None, serviceEntry=None):
    
    switchValue = resolve_value(statementNode.parameter, request)
    switchValue = switchValue if switchValue else ''
    inSwitch = False
    caseFound = False

    for cbChild in statementNode.children:

        if 'break' == cbChild.cmd:
            inSwitch = False
            continue

        if 'case' == cbChild.cmd:
            caseParameter = resolve_value(cbChild.parameter, request)
            caseParameter = caseParameter if caseParameter else ''
            if caseParameter == switchValue:
                caseFound = True
                inSwitch = True
            else :
                inSwitch = inSwitch or False
 
        if 'default' == cbChild.cmd:
            inSwitch = not caseFound or inSwitch
            continue

        if inSwitch:
            r = processCommandBlock(cbChild, request, currentResult, serviceEntry)
            currentResult = results_coalesce(r, currentResult)
            
    return currentResult


Commands.Registry['switch'] = switch_command


#
# foreach_command
#
def foreach_command (request, currentResult=None, statementNode=None, serviceEntry=None):
    indexResult = processCommand(statementNode.parameter, request, currentResult, serviceEntry)

    if indexResult is not None and len(indexResult.table) > 0:
        list_ = indexResult.toList()
        iRequest = copy.deepcopy(request)
    
        for map_ in list_:
            iRequest.parameters.update(map_)
            for child in statementNode.children:
                r = processCommandBlock(child, iRequest, currentResult, serviceEntry)
                currentResult = results_coalesce(r, currentResult)
    return currentResult;


Commands.Registry['foreach'] = foreach_command


#
# while_command
#
def while_command(request, currentResult=None, statementNode=None, serviceEntry=None):
    counter = 0;
    while counter < MAX_WHILE:
        whileCondition = resolve_value(statementNode.parameter, request)
        if not whileCondition:
            break
        counter += 1
        for  child in statementNode.children:
            r = processCommandBlock(child, request, currentResult, serviceEntry)
            currentResult = results_coalesce(r, currentResult)
    return currentResult


Commands.Registry['while'] = while_command


#
# add_role_command
#
def add_role_command(request, currentResult=None, statementNode=None, serviceEntry=None):
    if statementNode.parameter: request.roles.add(statementNode.parameter)
    return currentResult


Commands.Registry['add-role'] = add_role_command


#
# remove_role_command
#
def remove_role_command(request, currentResult=None, statementNode=None, serviceEntry=None):
    if statementNode.parameter: request.roles.discard(statementNode.parameter)
    return currentResult


Commands.Registry['remove-role'] = remove_role_command


#
# comment_command
#
def comment_command(request, currentResult=None, statementNode=None, serviceEntry=None):
    if statementNode.parameter:
        logger.info(M('comment:', statementNode.parameter))
        ProcessLog.Current().infoUser(statementNode.parameter)
    return currentResult


Commands.Registry['comment'] = comment_command


def closeQuietly(o):
    if o:
        try:
            o.close()
        except Exception as e:
            logger.error(e, exc_info=True) 

            
#
# tokenize 
#
def tokenize(s, delimiter=DEFAULT_DEL, escape=DEFAULT_ESC, keepEmpty=True, strip=True):
    if not s:
        return []
    tokens = []
    buf = ''
    inescape = False
    for c in s:
        if inescape:
            buf += c
            inescape = False
            continue
        if c == delimiter:
            if strip:
                buf = buf.strip()
            if buf or keepEmpty:
                tokens.append(buf)
            buf = ''
            continue
        if c == escape:
            inescape = True
            continue
        buf += c     
    if strip:
        buf = buf.strip()
    if buf or keepEmpty:
        tokens.append(buf)
          
    return tokens


def escape(s, delimiter=',', escape='\\'):
        res = ''
        if s:              
            for c in s:
                if c == delimiter or c == escape:
                    res += escape
                res += c              
        return res


def joinTokens(list_, delimiter=DEFAULT_DEL, escape=DEFAULT_ESC):
        res = ''
        if  list_:
            i = 0
            for e in list_:
                s = _escape(str(e), delimiter, escape)
                if i > 0:
                    res += delimiter
                res += s
                i += 1
        return res;


def _escape(s, d, esc) :
    res = ''
    for c in s:
        if c == d or c == esc:
            res += esc 
        res += c
    return res

    
#
# to_camel_case --- Thanks to Stackoverflow
#
def to_camel_case(snake_str):
    components = snake_str.lower().split('_')
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return components[0] + ''.join(x.title() for x in components[1:])


#
# processRqSqlText
#
def processRqSqlText(text='', source='', connection=None):
    counter = 0
    parameters = {}
    statements = ''
    if connection:
        pass
    try:
        lines = tokenize(text, delimiter='\n', strip=False, keepEmpty=True)
        inComment = False
        inStatement = False
        for lineOrig in lines:
            line = lineOrig.strip()
            # comment
            if line[:2] == '--':
                if not inComment:
                    if inStatement:
                        saveRQService(parameters, statements, source)
                        statements = ''
                        parameters = {}
                        inStatement = False
                        counter += 1
                inComment = True
                processParameter(parameters, line[2:].strip())
            else:                
                inComment = False
                inStatement = True
                statements += lineOrig + '\n';
        
        if inStatement:
            saveRQService(parameters, statements, source)
            counter += 1

    except Exception as e:
        logger.error(M('Error in:', source, '->', e))
    logger.info(M(source, ': sql statements done:', counter))


def saveRQService(parameters, statements, source):
    parameters['source'] = source
    parameters['statements'] = statements.strip()
    Request(saveServiceId, parameters, roles=set(['SYSTEM'])).run()


def processParameter(parameters, line):
    p = tokenize(line, delimiter='=', strip=True)
    if len(p) > 1:
        name = p[0]
        value = p[1]
        parameters[name] = value


def processSqlText(connection=None, text='', source=''):
    counter = 0
    lines = tokenize(text, delimiter='\n', strip=True, keepEmpty=False)
    sql = ''
    for _, line in enumerate(lines): 
        # comment
        if line[:2] == '--' or not line:
            continue
        
        # sql end
        if line[-1:] == ';':
            sql += line[:-1]
            r = processSql_con(connection, sql)
            logger.debug(M('Update count is:' , r.rowsAffected , 'on:' , sql))
            counter += 1
            sql = ''
        else:    
            sql += line + '\n'
            
    logger.info(M(source , ':' , counter, ' sqls done.'));
    return counter


def currentTimeMillis():
    return int(round(time.time() * 1000))

#
# QueryAndParams
#


class QueryAndParams():

    def __init__(self, named_query, req_params):
        
        self.named_query = named_query
        self.req_params = req_params
        
    def convertQuery(self):
    
        self.qm_query = ''
        self.in_param = False

        self.param_list = []
        self.current_param = ''
        
        prot = False

        for c in self.named_query:
            if not prot:
                if self.in_param:
                    if c.isalnum() or c == '_' or c == '$' or c == '[' or c == ']':
                        self.current_param += c
                        continue
                    else:
                        self._process_param()
                if not self.in_param and c == ':':
                    self.in_param = True
                    continue
    
            if c == '\'':
                prot = not prot
            self.qm_query += c
        # end processing
        if self.in_param:
            self._process_param()
        #

    def _process_param(self):
        param = self.current_param
        if (param[-2:] == '[]'):
            param_base = param[:-2]
            a_value = self.req_params.get(param_base, None) # self.req_params[param_base] if param_base in self.req_params else None
            if a_value is None:
                self.param_list.append(param)
                self.qm_query += '%s'
            else:
                request_values = a_value.split(',')
                index = 0
                for request_value in request_values:
                    param_indexed = '%s[%s]' % (param_base , index)
                    self.req_params[param_indexed] = request_value
                    self.param_list.append(param_indexed)
                    if index == 0:
                        self.qm_query += '%s'
                    else:
                        self.qm_query += ',%s'
                    index += 1
        else:
            self.param_list.append(param)
            self.qm_query += '%s'
        self.current_param = ''
        self.in_param = False
        

def removeWS(s):
    return ''.join([c for c in s if not c.isspace()])


def parse_statement(statement):
        statement = statement.strip()
        if not statement:
            return None, None, None
        
        first_ws = len(statement)
        i = 0
        for ch in statement:
            if ch.isspace():
                first_ws = i
                break
            i += 1;
        cmd = statement[0:first_ws].strip()
        parameters = ''
        if first_ws != len(statement):
            parameters = statement[first_ws:].strip()
            
        if Commands.isCmd(cmd):
            return cmd, parameters, statement
        
        return 'sql', statement, statement

    
def assertStatementNodeEquals(expected, actual):
    if not expected.cmd == actual.cmd:
        return False
    if not len(expected.children) == len(actual.children):
        return False
    for exp_ch, act_ch in zip(expected.children, actual.children):
        if not assertStatementNodeEquals(exp_ch, act_ch) == True:
            return False
    return True


print('remote_query DONE')
