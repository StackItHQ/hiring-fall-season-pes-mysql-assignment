import hashlib


def get_sql_rows(sql, table_name):
    try:
        query = f"SELECT * FROM {table_name};"
        sql.execute(query)
        rows = sql.fetchall()
        for i in range(len(rows)):
            rows[i] = list(rows[i])
            rows[i] = [j if j else '' for j in rows[i]]
        return list(rows)
    except:
        return []


def get_sql_headers(sql, table_name):
    try:
        query = f"SELECT * FROM {table_name};"
        sql.execute(query)
        _ = sql.fetchall()
        headers = [i[0] for i in sql.description]
        headers = [i if i != 'NA' else '' for i in headers]
        return headers
    except:
        return[]

def get_google_headers(rows):
    headers = rows.pop(0)
    headers = [i if i != '' else 'NA' for i in headers]
    return headers

def get_google_rows(rows):
    rows = [[i if i != '' else None for i in row] for row in rows]
    return rows


def hash_list(input_list):
    concatenated_string = ''.join(str(item) for item in input_list)
    concatenated_bytes = concatenated_string.encode('utf-8')
    hash_object = hashlib.sha256(concatenated_bytes)
    return hash_object.hexdigest()


def update_sql(data, table_name, sql, conn):

    # print('sql should be changed')
    headers = get_google_headers(data)
    rows = get_google_rows(data)
    query = f"DROP TABLE IF EXISTS {table_name};"
    sql.execute(query)
    conn.commit()
    if headers or rows:
        query = f"CREATE TABLE {table_name} (" + ", ".join([f"`{header}` VARCHAR(255)" for header in headers if header]) + ");"
        sql.execute(query)
        query = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({', '.join(['%s'] * len(headers))});"
        for row in rows:
            sql.execute(query, row)
        conn.commit()


def update_google(table, data):
    # print('google should be changed')
    table.clear()
    table.update(values=data, range_name='A1')