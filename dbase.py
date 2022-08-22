import pymysql


# +WORK WITH DATABASE
def get_all_records_from_table(connection_dbase_data, table_name):
    try:
        connection = pymysql.connect(
            host=connection_dbase_data[0],
            port=connection_dbase_data[1],
            user=connection_dbase_data[2],
            password=connection_dbase_data[3],
            database=connection_dbase_data[4]
            # cursorclass=pymysql.cursors.DictCursor
        )
        print('DBASE connected {0}...'.format(connection_dbase_data[4]))

        try:
            with connection.cursor() as cursor:
                sql = "SELECT * FROM {0}".format(table_name)
                cursor.execute(sql)
                res = cursor.fetchall()
                return res
        finally:
            connection.close()

    except Exception as ex:
        print("Connection refused...")
        print(ex)


def select_sql(connection_dbase_data, sql_request):
    try:
        connection = pymysql.connect(
            host=connection_dbase_data[0],
            port=connection_dbase_data[1],
            user=connection_dbase_data[2],
            password=connection_dbase_data[3],
            database=connection_dbase_data[4]
            # cursorclass=pymysql.cursors.DictCursor
        )
        print('DBASE connected {0}...'.format(connection_dbase_data[4]))

        try:
            with connection.cursor() as cursor:
                cursor.execute(sql_request)
                res = cursor.fetchall()
                return res
        finally:
            connection.close()

    except Exception as ex:
        print("Connection refused...")
        print(ex)


def update_sql(connection_dbase_data, sql_update_text):
    try:
        connection = pymysql.connect(
            host=connection_dbase_data[0],
            port=connection_dbase_data[1],
            user=connection_dbase_data[2],
            password=connection_dbase_data[3],
            database=connection_dbase_data[4]
            # cursorclass=pymysql.cursors.DictCursor
        )
        print('DBASE connected...')

        try:
            with connection.cursor() as cursor:
                sql = sql_update_text
                cursor.execute(sql)
                connection.commit()
        finally:
            connection.close()

    except Exception as ex:
        print("Connection refused...")
        print(ex)


# example
# add_record_to_table(ConnectionDbaseData, 'tickers', TickerName=ticker_name, ShareName=share_name)
def add_record_to_table(connection_dbase_data, table_name, **data):
    try:
        connection = pymysql.connect(
            host=connection_dbase_data[0],
            port=connection_dbase_data[1],
            user=connection_dbase_data[2],
            password=connection_dbase_data[3],
            database=connection_dbase_data[4]
            # cursorclass=pymysql.cursors.DictCursor
        )
        print('DBASE connected...')

        fields = '`'
        values = '\''
        count_params = data.__len__()
        for key, value in data.items():
            if key != None:
                fields = fields + key + '`, `'
            else:
                fields = key + " " + '\', \''
            if value != None:
                values = values + value + '\', \''
            else:
                values = values + " " + '\', \''
        fields = fields[0:len(fields)-3]
        values = values[0:len(values)-3]

        try:
            with connection.cursor() as cursor:
                sql = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(table_name, fields, values)
                cursor.execute(sql)
                connection.commit()
        finally:
            connection.close()

    except Exception as ex:
        print("Connection refused...")
        print(ex)


def is_unique_record(connection_dbase_data, table_name, key_field, key_field_value):
    try:
        connection = pymysql.connect(
            host=connection_dbase_data[0],
            port=connection_dbase_data[1],
            user=connection_dbase_data[2],
            password=connection_dbase_data[3],
            database=connection_dbase_data[4]
            # cursorclass=pymysql.cursors.DictCursor
        )
        print('DBASE connected...')

        try:
            with connection.cursor() as cursor:
                sql = "SELECT * FROM {0} WHERE {1} = '{2}'".format(table_name, key_field, key_field_value)
                cursor.execute(sql)
                res = cursor.rowcount
                if res == 0:
                    return 1
                else:
                    return 0
        finally:
            connection.close()

    except Exception as ex:
        print("Connection refused...")
        print(ex)


def is_unique_record2(connection_dbase_data, table_name, **data):
    try:
        connection = pymysql.connect(
            host=connection_dbase_data[0],
            port=connection_dbase_data[1],
            user=connection_dbase_data[2],
            password=connection_dbase_data[3],
            database=connection_dbase_data[4]
            # cursorclass=pymysql.cursors.DictCursor
        )
        print('DBASE connected...')

        fields = '`'
        values = '\''
        sql_condition = ""
        count_params = data.__len__()
        for key, value in data.items():
            sql_condition = sql_condition + key + " = " + '\'' + value + '\'' + " and "
            # fields = fields + key + '`, `'
            # values = values + value + '\', \''
        # fields = fields[0:len(fields)-3]
        # values = values[0:len(values)-3]
        sql_condition = sql_condition[0:len(sql_condition)-4]

        try:
            with connection.cursor() as cursor:
                sql = "SELECT * FROM {0} WHERE {1}".format(table_name, sql_condition)
                cursor.execute(sql)
                res = cursor.rowcount
                if res == 0:
                    return 1
                else:
                    return 0
        finally:
            connection.close()

    except Exception as ex:
        print("Connection refused...")
        print(ex)


def call_stored_proc(connection_dbase_data, proc_name):
    try:
        connection = pymysql.connect(
            host=connection_dbase_data[0],
            port=connection_dbase_data[1],
            user=connection_dbase_data[2],
            password=connection_dbase_data[3],
            database=connection_dbase_data[4]
            # cursorclass=pymysql.cursors.DictCursor
        )
        print('DBASE connected in ... call_stored_proc')

        try:
            with connection.cursor() as cursor:
                res = cursor.callproc(proc_name)
        finally:
            connection.close()

    except Exception as ex:
        print("Connection refused in ... call_stored_proc")
        print(ex)


# select all unsent records
def GetDealsFromFinViz(connection_dbase_data):
    try:
        connection = pymysql.connect(
            host=connection_dbase_data[0],
            port=connection_dbase_data[1],
            user=connection_dbase_data[2],
            password=connection_dbase_data[3],
            database=connection_dbase_data[4]
            # cursorclass=pymysql.cursors.DictCursor
        )
        print('DBASE connected...')

        try:
            with connection.cursor() as cursor:
                sql = "SELECT * FROM DealFromFinViz WHERE DataSended = 0"
                cursor.execute(sql)
                res = cursor.fetchall()
                return res
        finally:
            connection.close()

    except Exception as ex:
        print("Connection refused...")
        print(ex)


def GetAllDealsFromFinViz(connection_dbase_data):
    try:
        connection = pymysql.connect(
            host=connection_dbase_data[0],
            port=connection_dbase_data[1],
            user=connection_dbase_data[2],
            password=connection_dbase_data[3],
            database=connection_dbase_data[4]
            # cursorclass=pymysql.cursors.DictCursor
        )
        print('DBASE connected...')

        try:
            with connection.cursor() as cursor:
                sql = "SELECT * FROM DealFromFinViz"
                cursor.execute(sql)
                res = cursor.fetchall()
                return res
        finally:
            connection.close()

    except Exception as ex:
        print("Connection refused...")
        print(ex)


def SetFlagDealToSent(connection_dbase_data, id_deal):
    try:
        connection = pymysql.connect(
            host=connection_dbase_data[0],
            port=connection_dbase_data[1],
            user=connection_dbase_data[2],
            password=connection_dbase_data[3],
            database=connection_dbase_data[4]
            # cursorclass=pymysql.cursors.DictCursor
        )
        print('DBASE connected...')

        try:
            with connection.cursor() as cursor:
                sql = "UPDATE DealFromFinViz SET DataSended = 1 WHERE DataSended = 0 AND idDeals = {0}".format(id_deal)
                cursor.execute(sql)
                connection.commit()
        finally:
            connection.close()

    except Exception as ex:
        print("Connection refused...")
        print(ex)


def GetAllUsers(connection_dbase_data):
    try:
        connection = pymysql.connect(
            host=connection_dbase_data[0],
            port=connection_dbase_data[1],
            user=connection_dbase_data[2],
            password=connection_dbase_data[3],
            database=connection_dbase_data[4]
            # cursorclass=pymysql.cursors.DictCursor
        )
        print('DBASE connected...')

        try:
            with connection.cursor() as cursor:
                sql = "SELECT * FROM users"
                cursor.execute(sql)
                res = cursor.fetchall()
                return res
        finally:
            connection.close()

    except Exception as ex:
        print("Connection refused...")
        print(ex)


def AddUserToDB(connection_dbase_data, first_name, last_name, chat_id):
    try:
        connection = pymysql.connect(
            host=connection_dbase_data[0],
            port=connection_dbase_data[1],
            user=connection_dbase_data[2],
            password=connection_dbase_data[3],
            database=connection_dbase_data[4]
            # cursorclass=pymysql.cursors.DictCursor
        )
        print('DBASE connected...')

        try:
            with connection.cursor() as cursor:
                sql = "INSERT INTO Users (`UserName`, `UserLastName`, `IdChat`) VALUES ('{0}', '{1}', '{2}')".format(first_name, last_name, chat_id)
                cursor.execute(sql)
                connection.commit()
        finally:
            connection.close()

    except Exception as ex:
        print("Connection refused...")
        print(ex)
# -WORK WITH DATABASE
