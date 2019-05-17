"""Tools for reading CoLTE database log entries
"""

import mysql.connector


class ColteReader(object):
    def __init__(self, db_host, db_user, db_name, db_password):
        self._cnx = mysql.connector.connect(
            host=db_host,
            user=db_user,
            database=db_name,
            passwd=db_password,
        )

    def close(self):
        self._cnx.close()

    class _CursorIterator(object):
        def __init__(self, connection, query):
            self._connection = connection
            self._query = query

        def __enter__(self):
            self._cursor = self._connection.cursor()
            self._cursor.execute(self._query)
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self._cursor.close()

        def __iter__(self):
            return self

        def __next__(self):
            return self._cursor.__next__()

    def stage_flow_logs(self):
        self._cnx.start_transaction(isolation_level="SERIALIZABLE")
        try:
            cursor = self._cnx.cursor()
            cursor.execute("drop table flowlogs_staging; "
                           "create table flowlogs_staging "
                           "as (select * from flowlogs);")
            self._cnx.commit()
        except Exception as e:
            self._cnx.rollback()
            raise e

        return self._CursorIterator(connection=self._cnx,
                                    query="select * from flowlogs_staging;")

    def purge_currently_staged_flowlogs(self):
        self._cnx.start_transaction(isolation_level="SERIALIZABLE")
        try:
            cursor = self._cnx.cursor()
            cursor.execute("DELETE FROM flowlogs "
                           "WHERE EXISTS ("
                           "SELECT * FROM flowlogs, flowlogs_staging as staging"
                           "WHERE flowlogs.intervalStart = staging.intervalStart "
                           "AND flowlogs.intervalStop = staging.intervalStop "
                           "AND flowlogs.addressA = staging.addressA "
                           "AND flowlogs.addressB = staging.addressB "
                           "AND flowlogs.transportProtocol = staging.transportProtocol "
                           "AND flowlogs.portA = staging.portA "
                           "AND flowlogs.portB = staging.portB "
                           "AND flowlogs.bytesAtoB = staging.bytesAtoB "
                           "AND flowlogs.bytesBtoA = staging.bytesBtoA);")
            self._cnx.commit()
        except Exception as e:
            self._cnx.rollback()
            raise e

    def flow_logs(self):
        return self._CursorIterator(connection=self._cnx,
                                    query="select * from flowlogs;")

    def dns_logs(self):
        query_string = "select time, srcIp, dstIp, transportProtocol, srcPort, dstPort, opcode, resultcode, host, ip_addresses, ttls, idx from dnsResponses, answers where dnsResponses.answer=answers.idx;"

        return self._CursorIterator(connection=self._cnx, query=query_string)

    def ip_imsi_table(self):
        return self._CursorIterator(connection=self._cnx,
                                    query="select * from static_ips")
