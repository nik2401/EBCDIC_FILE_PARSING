try:
    import sys
    import pyodbc
    import pandas as pd
    import numpy as np
    from Logger import Logger
    import multiprocessing
    from multiprocessing import Pool
except ModuleNotFoundError as err:
    print(str({err.name.strip()})," module is missing, Command to Install - {"" pip install ", str(err.name.strip()), "}")
    sys.exit(1)

######################################################################################################################################################

logger = Logger()

######################################################################################################################################################


def insert_chunk(Connection_String, chunk, Col_Header, table_name):
    try:
        connection = pyodbc.connect(Connection_String)
        cursor = connection.cursor()

        placeholders = ', '.join(['?' for _ in range(len(Col_Header))])
        qry = f"Insert into {table_name} ({', '.join(Col_Header)}) VALUES ({placeholders});"

        rows = [tuple(row) for _, row in chunk.iterrows()]
        cursor.executemany(qry, rows)

        connection.commit()
    except Exception as e:
        connection.rollback()
        logger.debug(f"insert_chunk Error occurred while inserting {e}", True)
        logger.log_exception(*sys.exc_info())
    finally:
        cursor.close()
        connection.close()

######################################################################################################################################################

def insert_to_Sql(Connection_String, List_Value, Col_Header, table_name):
    try:
        df = pd.DataFrame(List_Value, columns=Col_Header)
        num_cores = min(multiprocessing.cpu_count(), len(List_Value))
        splits = np.array_split(df, num_cores)

        with Pool(num_cores) as pool:
            pool.starmap(insert_chunk, [(Connection_String, chunk, Col_Header, table_name) for chunk in splits])

    except Exception as e:
        logger.debug(f"insert_to_Sql Error occurred while inserting {e}", True)
        logger.log_exception(*sys.exc_info())
    finally:
        del df

######################################################################################################################################################


"""
# Below code is to insert using SQLAlchemy, pandas and multiprocessing either use above code that use Pyodbc
# Just Un-Comment the code to change working process as per your requirement
try:
    import urllib
    import sqlalchemy as sa
    import sys
    import pandas as pd
    import numpy as np
    from Logger import Logger
    import multiprocessing
    from multiprocessing import Pool
    # package should be install, if not [Command: pip install modulename]
except ModuleNotFoundError as err:
    print(str({err.name.strip()})," module is missing, Command to Install - {"" pip install ", str(err.name.strip()), "}")
    sys.exit(1)

######################################################################################################################################################

logger = Logger()

######################################################################################################################################################

def insert_chunk(chunk, table_name, Connection_String):
    try:
        connection_uri = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(Connection_String)}"
        engine = sa.create_engine(connection_uri,echo=False, fast_executemany=True, poolclass=sa.pool.QueuePool)
        with engine.connect() as connection:
            chunk.to_sql(table_name, connection, schema=".dbo", if_exists="append", index=False, chunksize = 2000)
    except Exception as e:
        logger.debug(f"insert_chunk Error occurred while inserting {e}",True)
        logger.log_exception(*sys.exc_info())

######################################################################################################################################################

def insert_to_Sql(Connection_String, List_Value, Col_Header, table_name):
    try:
        df = pd.DataFrame(List_Value, columns = Col_Header)
        num_cores = min(multiprocessing.cpu_count(), len(List_Value))
        splits = np.array_split(df, num_cores)

        with Pool(num_cores) as pool:
            pool.starmap(insert_chunk, [(chunk, table_name, Connection_String) for chunk in splits])

    except Exception as e:
        logger.debug(f"insert_to_Sql Error occurred while inserting {e}",True)
        logger.log_exception(*sys.exc_info())
    finally:
        del df
        
######################################################################################################################################################
"""