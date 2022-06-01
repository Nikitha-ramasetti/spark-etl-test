from python_src.db_connect import *
from python_src.extract_transform import *


def extract_response():
    # extraction the response requests
    endpoint1 = request_url("https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users")

    endpoint2 = request_url("https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/messages")

    if ((endpoint1 == 'Not found') or (endpoint2 == 'Not found')):
        print("endpoint not found")
    else:
        print("response file loaded")

    return [endpoint1, endpoint2]


# Saving each dataframe in a separate variable executing functions
def transform(endpoint):
    users_DF = users(endpoint[0])
    subscriptions_DF = subscriptions(endpoint[0])
    messages_DF = messages(endpoint[1])
    print("DataFrames created")
    return [users_DF, subscriptions_DF, messages_DF]


def load(dframe, table_name, conn_str):
    dframe.to_sql(table_name, conn_str, if_exists='append', index=False)


#main
endpoint = extract_response()
res_df_list = transform(endpoint)

conn, conn_str = getConnection('postgres', 'postgres', 'localhost', '5433')
execute_sql_ddl_cmds(conn)

load(res_df_list[0], 'users', conn_str)
load(res_df_list[1], 'subscriptions', conn_str)
load(res_df_list[2], 'messages', conn_str)

print("Data inserted using to_sql() done successfully")
print("Finish ETL")