from flask import Flask, json
import pandas
import time
from datetime import datetime
import pyodbc as im
import numpy as np
import requests
import sys,os
app = Flask(__name__)
@app.route('/')
def index():
    return str(datetime.now())

@app.route('/log/<param>', methods=['GET'])
def get_log(param):
    result = []
    arrProcess = ["adhcisrep.dim_addr","adhcisrep.fct_hh_addr","adhcisrep.fct_hh_cert", "adhcisrep.fct_hh_cert_addr"]
    try:
        sqlquery = "select * from ({query}) q order by 2 desc "
        sqlformat = "select * from adhcisrep.audit_job where left(start_time,10)='{date} and lower(jobname) like '%{job}%' "

        for iP in arrProcess:
            if  (sql !=""):
                sql +=" union all "
            sql =  sqlformat.format(date=param,job=iP.lower())



        connect = im.connect('DSN=Implala', autocommit=True)
        df = pandas.read_sql(sqlquery.format(query=sql),connect)
        print(df)
        for i in df.head(100).index:
            job = str(df['jobname'][i])
            start_time = str(df['start_time'][i])
            end_time = str(df['end_time'][i])
            time_used = str(df['time_used'][i])
            data = {}
            data['job_name'] = job
            data['start_time'] = start_time
            data['end_time'] = end_time
            data['time_used'] = time_used
            result.append(data)

    except Exception as e:
        print('Error! Code: {c}, Message, {m}'.format(c=type(e).__name__, m=str(e)))
    finally:
        connect.close()
        df = None
        pd = None

    return json.dumps(result)

@app.route('/log/', methods=['GET'])
def get_log_all():
    result = []
    arrProcess = ["adhcisrep.dim_addr", "adhcisrep.fct_hh_addr", "adhcisrep.fct_hh_cert", "adhcisrep.fct_hh_cert_addr","adhcisrep.hh_product_detail"]
    try:
        sql=""
        sqlquery = "select * from ({query}) q order by 2 desc "
        sqlformat = "select * from adhcisrep.audit_job where  lower(jobname) like '%{job}' limit 20"

        for iP in arrProcess:
            if (sql != ""):
                sql += " union all "
            sql += sqlformat.format(job=iP.lower())
        print(sql)
        print(sqlquery.format(query=sql))
        connect = im.connect('DSN=Implala', autocommit=True)
        df = pandas.read_sql(sqlquery.format(query=sql), connect)
        print(df)
        for i in df.head(100).index:
            job = str(df['jobname'][i])
            start_time = str(df['start_time'][i])
            end_time = str(df['end_time'][i])
            time_used = str(df['time_used'][i])
            data = {}
            data['job_name'] = job
            data['start_time'] = start_time
            data['end_time'] = end_time
            data['time_used'] = time_used
            result.append(data)

    except Exception as e:
        print('Error! Code: {c}, Message, {m}'.format(c=type(e).__name__, m=str(e)))
    finally:
        connect.close()
        df = None
        pd = None

    return json.dumps(result)


@app.route('/logs/<param>', methods=['GET'])
def get_logs(param):
    result = []
    try:
        sql = "select * from adhcisrep.audit_job where  left(start_time,10)='" + param + "' order by 2 desc"
        connect = im.connect('DSN=Implala', autocommit=True)
        df = pandas.read_sql(sql,connect)
        print(df)
        for i in df.head(100).index:
            job = str(df['jobname'][i])
            start_time = str(df['start_time'][i])
            end_time = str(df['end_time'][i])
            time_used = str(df['time_used'][i])
            data = {}
            data['job_name'] = job
            data['start_time'] = start_time
            data['end_time'] = end_time
            data['time_used'] = time_used
            result.append(data)

    except Exception as e:
        print('Error! Code: {c}, Message, {m}'.format(c=type(e).__name__, m=str(e)))
    finally:
        connect.close()
        df = None
        pd = None

    return json.dumps(result)

@app.route('/logs/', methods=['GET'])
def get_logs_all():
    result = []
    try:
        sql = "select * from adhcisrep.audit_job order by 2 desc"
        connect = im.connect('DSN=Implala', autocommit=True)
        df = pandas.read_sql(sql,connect)
        print(df)
        for i in df.head(100).index:
            job = str(df['jobname'][i])
            start_time = str(df['start_time'][i])
            end_time = str(df['end_time'][i])
            time_used = str(df['time_used'][i])
            data = {}
            data['job_name'] = job
            data['start_time'] = start_time
            data['end_time'] = end_time
            data['time_used'] = time_used
            result.append(data)

    except Exception as e:
        print('Error! Code: {c}, Message, {m}'.format(c=type(e).__name__, m=str(e)))
    finally:
        connect.close()
        df = None
        pd = None

    return json.dumps(result)

#host='0.0.0.0',


@app.route('/hadoop/listtable/<database>', methods=['GET'])
def get_hadoop_listtable(database):
    result = []
    djobtime = datetime.today()
    try:
        message = ""
        messagerow = ""
        connection = im.connect('DSN=Implala', autocommit=True)
        print(connection)
        t0 = time.time()

        now = datetime.now()
        nowts = now.strftime('%Y-%m-%dT%H:%M:%S') + ('-%02d' % (now.microsecond / 10000))
        print(nowts + '============= end process source query ============= ')
        print(f"elapsed time {(time.time() - t0):0.1f} seconds")

        print(nowts + '============= start process target  query ============= ')
        sqltable = "show tables in " + database
        print('============= sql command ============= ' + '\n' + sqltable)

        cursorresult = connection.cursor()
        cursorresult.execute(sqltable)
        result = cursorresult.fetchall()

        now = datetime.now()

        nowts = now.strftime('%Y-%m-%dT%H:%M:%S') + ('-%02d' % (now.microsecond / 10000))
        print(nowts + '============= end process  target  query ============= ')
        print(f"elapsed time {(time.time() - t0):0.1f} seconds")
        rerunprovince = ""
        isize = 0
        dsize = 0
        ilen = 0
        for s in result:
            table = s[0]
            try:
                tablefullname = database + "." + table
                sql = "show table stats " + tablefullname
                print(sql)
                cursor = connection.cursor()
                cursor.execute(sql)
                stattable = cursor.fetchone()
                if stattable:
                    strow = str(stattable[0])
                    stfile = str(stattable[1])
                    stsize = stattable[2]
                    stbcache = stattable[3]
                    stscache = stattable[4]
                    stformat = stattable[5]
                    stincre = stattable[6]
                    stlocation = stattable[7]
                    messagerow += tablefullname + "," + stsize + "\n"
                    if (str(stsize).find("GB") > 0):
                        ilen = str(stsize).find("GB")
                        isize = 1073741824
                        dsize = float(str(stsize)[0:ilen]) * isize
                    elif (str(stsize).find("MB") > 0):
                        ilen = str(stsize).find("MB")
                        isize = 1048576
                        dsize = float(str(stsize)[0:ilen]) * isize
                    elif (str(stsize).find("KB") > 0):
                        ilen = str(stsize).find("KB")
                        isize = 1024
                        dsize = float(str(stsize)[0:ilen]) * isize
                    elif (str(stsize).find("B") > 0):
                        ilen = str(stsize).find("B")
                        isize = 1
                        dsize = float(str(stsize)[0:ilen]) * isize

                    messagerow = tablefullname + "," + str(dsize) + "," + stsize
                    data = {}
                    data['table'] = tablefullname
                    data['size'] = str(dsize)
                    data['size(unit)'] = stsize
                    result.append(data)
                    print(messagerow)
                    message += tablefullname + "," + strow + "," + stfile + "," + stsize + "," + stbcache + "," + stscache + "," + stformat + "," + stincre + "," + stlocation
                    message += "\n"

            except Exception as ee:
                messagerow = tablefullname + ",0,not avalible"
                data = {}
                data['table'] = tablefullname
                data['size'] = "0"
                data['size(unit)'] = "not avalible"
                result.append(data)
                print('Error! Code: {c}, Message, {m}'.format(c=type(ee).__name__, m=str(ee)))
    except Exception as e:
        print('Error! Code: {c}, Message, {m}'.format(c=type(e).__name__, m=str(e)))
    finally:
        connection.close()
    return json.dumps(result)


@app.route('/hadoop/listcolumn/<table>', methods=['GET'])
def get_hadoop_listcolumn(table):
    try:
        message = ""
        messagerow = ""
        sqlTable = "select {column} from {table} t "
        connection = im.connect('DSN=Implala', autocommit=True)
        t0 = time.time()
        now = datetime.now()
        nowts = now.strftime('%Y-%m-%dT%H:%M:%S') + ('-%02d' % (now.microsecond / 10000))
        print(nowts + '============= end process source query ============= ')
        print(f"elapsed time {(time.time() - t0):0.1f} seconds")
        tablespecific = table
        sql = "SHOW column  stats "+tablespecific
        cursor = connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        columnname = ""
        columnnametype=""
        tablename = "t."
        iseq = 0
        HTML = ""
        htmlformat = "<tr>"
        htmlformat += "\n<td>{seq}</td>"
        htmlformat += "\n<td>{key}</td>"
        htmlformat += "\n<td>{column}</td>"
        htmlformat += "\n<td>{type}</td>"
        htmlformat += "\n<td>{description}</td>"
        htmlformat += "\n</tr>"

        for c in result:
            iseq += 1
            cname = c[0]
            type = c[1]

            if (columnname!=""):
                columnname +="\n,"
            if (columnnametype!=""):
                columnnametype +="\n,"
            columnname += tablename+(c[0])
            columnnametype += "cast("+ tablename + (c[0]) +" as " + type+") as "+(c[0])
            HTML = htmlformat.format(seq = str(iseq), key = "", column = cname, type = type,description="")
            print(HTML)
        print(columnnametype)
        print(columnname)
    except Exception as e:
        print('Error! Code: {c}, Message, {m}'.format(c=type(e).__name__, m=str(e)))
    finally:
        connection.close()

    return sqlTable.format(column=columnname,table=table)

@app.route('/hadoop/listcolumntype/<table>', methods=['GET'])
def get_hadoop_listcolumntype(table):
    try:
        message = ""
        messagerow = ""
        sqlTable = "select {column} from {table} t "
        connection = im.connect('DSN=Implala', autocommit=True)
        t0 = time.time()
        now = datetime.now()
        nowts = now.strftime('%Y-%m-%dT%H:%M:%S') + ('-%02d' % (now.microsecond / 10000))
        print(nowts + '============= end process source query ============= ')
        print(f"elapsed time {(time.time() - t0):0.1f} seconds")
        tablespecific = table
        sql = "SHOW column  stats "+tablespecific
        cursor = connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        columnname = ""
        columnnametype=""
        tablename = "t."
        iseq = 0
        HTML = ""
        htmlformat = "<tr>"
        htmlformat += "\n<td>{seq}</td>"
        htmlformat += "\n<td>{key}</td>"
        htmlformat += "\n<td>{column}</td>"
        htmlformat += "\n<td>{type}</td>"
        htmlformat += "\n<td>{description}</td>"
        htmlformat += "\n</tr>"

        for c in result:
            iseq += 1
            cname = c[0]
            type = c[1]
            if (str(type).lower().find("string")>-1 or str(type).lower().find("varchar")>-1):
                sqlmaxlength = "select max(length({column})) as max_length from {table}".format(column = cname , table = tablespecific)
                cursormax = connection.cursor()
                cursormax.execute(sqlmaxlength)
                resultmax = cursormax.fetchone()

                if resultmax:
                    maxtext = resultmax[0]
                    type = "varchar({ml})".format(ml=maxtext)

            if (columnname!=""):
                columnname +="\n,"
            if (columnnametype!=""):
                columnnametype +="\n,"
            columnname += tablename+(c[0])
            columnnametype += "cast("+ tablename + (c[0]) +" as " + type+") as "+(c[0])
            HTML = htmlformat.format(seq = str(iseq), key = "", column = cname, type = type,description="")
    except Exception as e:
        print('Error! Code: {c}, Message, {m}'.format(c=type(e).__name__, m=str(e)))
    finally:
        connection.close()

    return sqlTable.format(column=columnnametype,table=table)

@app.route('/query/<filename>', methods=['GET'])
def get_query(filename):
    result  = []
    query = ""
    formatpath = "{dir}\{file}"
    fullpath = ""
    value =""
    path = os.path.dirname(sys.argv[0])
    fullpath = formatpath.format(dir=path,file=filename)
    print(fullpath)
    if os.path.exists(fullpath):
        nowstart = datetime.now()
        nowstartstr = nowstart.strftime('%Y-%m-%dT%H:%M:%S') + ('-%02d' % (nowstart.microsecond / 10000))
        sjobname = "query-" + filename
        t0 = time.time()
        fh = open(fullpath, 'r', encoding='utf-8')
        query = fh.read()
        print(nowstartstr + "=" + sjobname + " \n query =" + query)
        fh.close()
        connect = im.connect('DSN=Implala', autocommit=True)
        df = pandas.read_sql(query, connect)
        for i in df.index:
            data = {}
            for j in df.columns:
                value = df[j][i]
                data[j] = str(value)
                result.append(data)

        print(f"elapsed time {(time.time() - t0):0.1f} seconds")
        return json.dumps(result, indent=4, ensure_ascii=False).encode('utf8')
    else:
        return "file not found"

@app.route('/fuzzy_ai/<thaiid>', methods=['GET'])
def set_fuzzy_by_thai(thaiid):

    partial = 0
    ratio = 0
    token = 0
    iloop = 0
    irow = 0

    i70 = 0
    i80 = 0
    i90 = 0
    ilow  = 0
    arrAddrKeySortAsc = []
    arrAddrKeySortDesc = []

    sql = " select  t.match_key from adhcisrep.tee_ww_prod_key_before t"
    sql += " where t.idnt_nbr = '{idnt_nbr}'"

    key1 = ""
    key2 = ""
    key3 = ""
    urlformat = "http://winthehouse.dwh-journey.arctic.true.th/fuzzy_compare/{compare}"
    findcompare = ""

    sTableQueryInsert = " INSERT into adhcisrep.tee_ww_prod_key_manual(idnt_nbr,source_addr_key,target_addr_key,partial_score,ratio_score,token_score,ppn_tm) VALUES "
    sIns = ""
    try:
        connection = im.connect('DSN=Implala', autocommit=True)
        print(connection)
        t0 = time.time()
        now = datetime.now()
        nowts = now.strftime('%Y-%m-%dT%H:%M:%S') + ('-%02d' % (now.microsecond / 10000))
        print(nowts + '============= start process  query ============= ')

        dsExeCursor = connection.cursor()

        dsCursor = connection.cursor()
        dsCursor.execute(sql.format(idnt_nbr=thaiid))
        print(sql.format(idnt_nbr=thaiid))
        result = dsCursor.fetchall()
        print(len(result))
        if len(result) > 0:
            sIns = ""
            arrAddrKeySortAsc = []
            arrAddrKeySortDesc = []
            for row in result:
                match_key = row[0]

                arrAddrKeySortAsc.append(match_key)
                arrAddrKeySortDesc.append(match_key)

        arrAddrKeySortAsc = list(dict.fromkeys(arrAddrKeySortAsc))
        arrAddrKeySortDesc = list(dict.fromkeys(arrAddrKeySortDesc))
        arrAddrKeySortAsc.sort()
        arrAddrKeySortDesc.sort(reverse=True)
        if (arrAddrKeySortAsc != None and arrAddrKeySortDesc != None):
            arrAddrCompare = []
            for itemasc in arrAddrKeySortAsc:
                key1 = itemasc
                for itemdesc in arrAddrKeySortDesc:
                    key2 = itemdesc
                    if (key1 != key2):
                        findcompare = key1 + "-" + key2
                        url = (urlformat.format(compare=findcompare))
                        print(url)
                        try:
                            response = json.loads(requests.get(url).text)
                        except:
                            response = None
                        if (response):
                            partial = (response['partial'])
                            ratio = (response['ratio'])
                            token = (response['token'])
                            compare = {"souce": key1, "target": key2, "ratio": ratio, "partial": partial,
                                       "token": token}

                        arrAddrCompare.append(compare)
                        iloop += 1
                        if (ratio >69 or partial >69 or token >69):
                            i70 +=1
                        elif (ratio >79 or partial >79 or token >79):
                            i80 += 1
                        elif (ratio >89 or partial >89 or token >89):
                            i90 += 1
                        else:
                            ilow +=1

                        if sIns != "":
                            sIns += ","
                        sIns += " ('" + thaiid + "', '" + key1 + "', '" + key2 + "', " + str(partial) + ", " + str(ratio) + ", " + str(token) + ",now()) "

                        fromatcounter = "Thai ID = {0} Qty , Loop = {1} Qty "
                        print(fromatcounter.format(str(thaiid), str(iloop)))

                        if (sIns != ""):
                            dsExeCursor.execute(sTableQueryInsert + sIns)
        #return json.dumps({'status': 'OK' ,'record' : str(iloop),'0-69' : str(ilow) ,'70-79' : str(i70) ,'80-89' : str(i80) ,'90-99' : str(i90)})
        return json.dumps({'status': 'OK' ,'record' : str(iloop),'table' : 'adhcisrep.tee_ww_prod_key_manual'})
    except Exception as e:
        print('Error! Code: {c}, Message, {m}'.format(c=type(e).__name__, m=str(e)))
    finally:
        djobtime = datetime.today()
        jobtime = djobtime.strftime('%d/%m/%Y %H:%M:%S')
        if connection: connection.close()


@app.route('/querytest', methods=['GET'])
def get_query_test():
     return json.dumps({'status':'OK'})
# host='0.0.0.0','172.19.234.74'
app.run(debug=True,port=5000)