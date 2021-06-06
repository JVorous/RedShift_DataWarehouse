import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description: this function drops a specified set of tables using
     the imported list 'drop_table_queries'

    Arguments:
        cur: the cursor object.
        conn: the connection object

    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        Description: this function creates a specified set of tables using
         the imported list 'create_table_queries'

        Arguments:
            cur: the cursor object.
            conn: the connection object

        Returns:
            None
        """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: Driver function. It creates the connection and then calls the drop and create functions
    in order. Then it closes the connection

    Arguments:
        None

    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
