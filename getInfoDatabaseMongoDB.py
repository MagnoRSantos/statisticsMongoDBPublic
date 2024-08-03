# -*- coding: utf-8 -*-

import os
import io
import re
import dotenv
import csv
import sqlite3
from datetime import datetime
from pymongo import MongoClient
from removeLogAntigo import removeLogs

## Carrega os valores do .env
dotenv.load_dotenv()

### Variaveis do local do script e log mongodb
dirapp = os.path.dirname(os.path.realpath(__file__))

## funcao que retorna data e hora Y-M-D H:M:S
def obterDataHora():
    datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return datahora


## funcao de gravacao de log
def GravaLog(strValue, strAcao):

    ## Path LogFile
    datahoraLog = datetime.now().strftime('%Y-%m-%d')
    pathLog = os.path.join(dirapp, 'log')
    pathLogFile = os.path.join(pathLog, 'logStatisticsMongoDB_{0}.txt'.format(datahoraLog))

    if not os.path.exists(pathLog):
        os.makedirs(pathLog)
    else:
        pass

    msg = strValue
    with io.open(pathLogFile, strAcao, encoding='utf-8') as fileLog:
        fileLog.write('{0}\n'.format(strValue))

    return msg


## funcao de conexao ao mongodb
def listDbAndCollMongoDB(p_nameCollection):

    ## grava log
    datahora = obterDataHora()
    msgLog = 'Obtendo dados estatisticos dos databases MongoDB (Inicio): {0}'.format(datahora)
    print(GravaLog(msgLog, 'a'))

    try:
       
       ## variaveis de conexao
        DBUSERNAME = os.getenv("USERNAME_MONGODB")
        DBPASSWORD = os.getenv("PASSWORD_MONGODB")
        MONGO_HOST = os.getenv("SERVER_MONGODB")
        DBAUTHDB   = os.getenv("DBAUTHDB_MONGODB")
        connstr = 'mongodb://' + DBUSERNAME + ':' + DBPASSWORD + '@' + MONGO_HOST + '/' + DBAUTHDB
        
        ## cria lista vazia
        listReturnMongoDb = []
        
        with MongoClient(connstr) as client:

            #=======================================================

            #listar todos databases
            cursor = client.list_database_names()

            for dbname in cursor:
                
                if re.search("^dat_", dbname):
                    
                    ## define database para uso no processo
                    dbCurrent = client[dbname]

                    ## obtem dados estatisticos do database
                    returnDbStats = dbCurrent.command("dbstats")
                    v_dbname = returnDbStats['db']
                    v_datEmpresa = v_dbname.replace('dat_', '')
                    v_qtdeCollections = returnDbStats['collections']
                    v_storageSizeMb = int(returnDbStats['storageSize'])/1048576
                    v_dataSizeMb = int(returnDbStats['dataSize'])/1048576

                    ## formata valor para 2 casas decimais
                    v_storageSizeMb = "{0:.2f}".format(v_storageSizeMb)
                    v_dataSizeMb    = "{0:.2f}".format(v_dataSizeMb)

                    ## obtem dados estatisticos da collection
                    # returnCollStats = dbCurrent.command("collstats", "Import")
                    returnCollStats = dbCurrent.command("collstats", p_nameCollection)
                    v_totalDocs = returnCollStats['count']

                    ## caso o campo avgObjSize nao exista assume o valor como 0 (zero)
                    # isso ocorre se a collection nao tiver nenhum documento
                    # formata valor para 2 casas decimais
                    v_avgSizeObject = returnCollStats['avgObjSize']/1024 if v_totalDocs > 0 else 0
                    v_avgSizeObject = "{0:.2f}".format(v_avgSizeObject)

              
                    # cria lista auxiliar vazia
                    listReturnMongoDbAux = []
                    
                    # insere valores na lista auxiliar
                    listReturnMongoDbAux.insert(0, v_dbname)
                    listReturnMongoDbAux.insert(1, v_datEmpresa)
                    listReturnMongoDbAux.insert(2, v_qtdeCollections)
                    listReturnMongoDbAux.insert(3, v_storageSizeMb)
                    listReturnMongoDbAux.insert(4, v_dataSizeMb)
                    listReturnMongoDbAux.insert(5, v_totalDocs)
                    listReturnMongoDbAux.insert(6, v_avgSizeObject)
                    
                    # insere na lista final
                    listReturnMongoDb.append(listReturnMongoDbAux)

    except Exception as e:
        msgException = "{0}".format(e)
        msgLog = 'Error: {0}'.format(msgException)
        print(GravaLog(msgLog, 'a'))

    finally:
        if not listReturnMongoDb:
            datahora = obterDataHora()
            msgLog = 'Nao foi possivel obter dados estatisticos do MongoDB\n'
            msgLog = '{0}***** Fim da aplicacao: {1}\n'.format(msgLog, datahora)
            print(GravaLog(msgLog, 'a'))
            exit()
        else:
            ## grava log
            datahora = obterDataHora()
            msgLog = 'Obtendo dados estatisticos dos databases MongoDB (Fim): {0}'.format(datahora)
            print(GravaLog(msgLog, 'a')) 
            
            ## chama funcao de insert no banco relacional, gravacao do arquivo csv e exibe em tela
            ## passando a lista como parametro
            gravaDadosSqlite(listReturnMongoDb)
            geraVisualizacaoCsv(listReturnMongoDb)
            gravaCSV(listReturnMongoDb)


## funcao de gravacao dos valores em arquivo csv
def gravaCSV(v_ListValuesMongoDB):

    file_csv = "statisticsMongoDB.csv"
    path_dir_csv = os.path.join(dirapp, 'csv')
    path_file_csv = os.path.join(path_dir_csv, file_csv)

    if not os.path.exists(path_dir_csv):
        os.makedirs(path_dir_csv)
    else:
        pass

    # field names 
    fields = ['dbname', 'datEmpresa', 'qtdeCollections', 'storageSizeMb', 'dataSizeMb', 'totalDocs', 'avgSizeObject'] 

    # apaga conteudo anterior e insere o atual
    with open(path_file_csv, 'w') as f:
        write = csv.writer(f)
        write.writerow(fields)
        write.writerows(v_ListValuesMongoDB)



## funcao apenas em caso de querer visualizar em formato csv
def geraVisualizacaoCsv(v_ListValuesMongoDB):

    # field names 
    fields = "dbname, datEmpresa, qtdeCollections, storageSizeMb, dataSizeMb, totalDocs, avgSizeObject"
    print(fields)
    GravaLog(fields , 'a')
    
    # loop de insert dos dados
    tamlist =  range(len(v_ListValuesMongoDB))
    for i in tamlist:
        dbname          = str(v_ListValuesMongoDB[i][0])
        datEmpresa      = str(v_ListValuesMongoDB[i][1])
        qtdeCollections = str(v_ListValuesMongoDB[i][2])
        storageSizeMb   = str(v_ListValuesMongoDB[i][3])
        dataSizeMb      = str(v_ListValuesMongoDB[i][4])
        totalDocs       = str(v_ListValuesMongoDB[i][5])
        avgSizeObject   = str(v_ListValuesMongoDB[i][6])

        resultCsv = "{0},{1},{2},{3},{4},{5},{6}".\
          format(dbname, datEmpresa, qtdeCollections, storageSizeMb, dataSizeMb, totalDocs, avgSizeObject)
        print(resultCsv)
        GravaLog(resultCsv, 'a')


## Funcao de criacao do database e tabela caso nao exista
def create_tables(dbname_sqlite3):
    
    ## script sql de criacao da tabela
    # pode ser adicionado a criacao de mais e uma tabeela
    # separando os scripts por virgulas
    sql_statements = [
        """
        CREATE TABLE "infoDatabaseMongoDb" (
            "infoDatabaseMongoDbId"	INTEGER NOT NULL UNIQUE,
            "Database"	TEXT NOT NULL,
            "Empresa"	TEXT NOT NULL,
            "QtdeCollections"	INTEGER,
            "StorageSizeMb"	NUMERIC,
            "DataSizeMb"	NUMERIC,
            "TotalDocumentos"	INTEGER,
            "MediaTamanhoObjetosKb"	NUMERIC,
            "DataExecucao"	REAL,
            PRIMARY KEY("infoDatabaseMongoDbId" AUTOINCREMENT)
        )        
        """
    ]

    # variaveis da conexão ao database
    path_dir_db = os.path.join(dirapp, 'db')
    path_full_dbname_sqlite3 = os.path.join(path_dir_db, dbname_sqlite3)
    
    # cria o diretorio caso nao exista
    if not os.path.exists(path_dir_db):
        os.makedirs(path_dir_db)
    else:
        pass
    

    try:
        with sqlite3.connect(path_full_dbname_sqlite3) as conn:
            cursor = conn.cursor()
            for statement in sql_statements:
                cursor.execute(statement)
            
            conn.commit()
    except sqlite3.Error as e:
        datahora = obterDataHora()
        msgException = "Error: {0}".format(e)
        msgLog = 'Criar tabela SQlite3 [infoDatabaseMongoDb] [Erro]: {0}\n{1}'.format(datahora, msgException)
        print(GravaLog(msgLog, 'a'))
    finally:
        msgLog = 'Criado tabela [infoDatabaseMongoDb] no database [{0}]'.format(dbname_sqlite3)
        print(GravaLog(msgLog, 'a'))

## gera comandos de inserts conforme valores da lista passada
def gravaDadosSqlite(v_ListValuesMongoDB):
    dbname_sqlite3 = os.getenv("DBNAME_SQLITE")
    path_dir_db = os.path.join(dirapp, 'db')
    path_full_dbname_sqlite3 = os.path.join(path_dir_db, dbname_sqlite3)
    RowCount = 0

    ## verifica se banco de dados existe 
    # caso não exista realizada a chamada da funcao de criacao
    if not os.path.exists(path_dir_db):
        create_tables(dbname_sqlite3)
    else:
        pass

    
    try:
        with sqlite3.connect(path_full_dbname_sqlite3) as conn:

            

            sqlcmd = '''
            INSERT INTO infoDatabaseMongoDb
                (Database, Empresa, QtdeCollections, StorageSizeMb, DataSizeMb, TotalDocumentos, MediaTamanhoObjetosKb, DataExecucao) 
            VALUES 
            (?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'));
            '''

            cur = conn.cursor()
            cur.executemany(sqlcmd, v_ListValuesMongoDB)
            RowCount = conn.total_changes
            conn.commit()
    
    except sqlite3.Error as e:
        datahora = obterDataHora()
        msgException = "Error: {0}".format(e)
        msgLog = 'Fim Insert tabela SQlite3 [infoDatabaseMongoDb] [Erro]: {0}\n{1}'.format(datahora, msgException)
        print(GravaLog(msgLog, 'a'))

    finally:
        msgLog = 'Quantidade de Registros Inseridos na tabela [infoDatabaseMongoDb]: {0} registro(s)'.format(RowCount)
        print(GravaLog(msgLog, 'a'))
    


## FUNCAO INICIAL
def main():
    ## log do inicio da aplicacao
    datahora = obterDataHora()
    msgLog = '\n***** Inicio da aplicacao: {0}'.format(datahora)
    print(GravaLog(msgLog, 'a'))
    
    ## chamada da funcao que obtem dados estatisticos do mongodb
    # passando a collection name como parametro
    v_nameCollection = "movies" 
    listDbAndCollMongoDB(v_nameCollection)
    
    ## remocao dos logs antigos acima de xx dias
    diasRemover = 30
    dirLog = os.path.join(dirapp, 'log')
    msgLog = removeLogs(diasRemover, dirLog)
    print(GravaLog(msgLog, 'a'))

    ## log do final da aplicacao
    datahora = obterDataHora()
    msgLog = '***** Final da aplicacao: {0}'.format(datahora)
    print(GravaLog(msgLog, 'a'))

#### inicio da aplicacao ####
if __name__ == "__main__":
    ## chamada da funcao inicial
    main()
