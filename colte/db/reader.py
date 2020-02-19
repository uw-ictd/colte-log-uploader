"""Tools for reading CoLTE database log entries
"""

import mysql.connector


class ColteReader(object):
    """Abstracts the CoLTE data store for higher level programs.

    On initialization it takes parameters to establish a connection with the
    underlying database.

    This class is NOT THREAD SAFE.
    """

    def __init__(self, db_host, db_user, db_name, db_password):
        """Initializes the ColteReader for use.

        After initialization the ColteReader must later be closed with the
        close() method.
        """
        self._cnx = mysql.connector.connect(
            host=db_host,
            user=db_user,
            database=db_name,
            passwd=db_password,
        )

    def close(self):
        """Closes the reader and releases any underlying resources.

        After close() has been called no further method calls may be made to
        the object.
        """
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
        """Stage flow log entries from the main log in a temporary table.

        This will clear and overwrite any currently staged logs with new logs
        from the main log store.

        Returns an iterator over the newly staged logs.
        """
        self._cnx.start_transaction(isolation_level="SERIALIZABLE")
        try:
            cursor = self._cnx.cursor()
            cursor.execute("drop table if exists flowStaging; "
                           "create table flowStaging "
                           "as (select * from flowlogs);")
            self._cnx.commit()
        except Exception as e:
            self._cnx.rollback()
            raise e

        return self._CursorIterator(connection=self._cnx,
                                    query="select * from flowStaging;")

    def purge_staged_flowlogs(self):
        """Delete any flow entries from the main log which have been staged."""
        self._cnx.start_transaction(isolation_level="SERIALIZABLE")
        try:
            cursor = self._cnx.cursor()
            cursor.execute("DELETE FROM flowlogs "
                           "WHERE EXISTS ("
                           "SELECT * FROM flowlogs, flowStaging "
                           "WHERE flowlogs.intervalStart = flowStaging.intervalStart "
                           "AND flowlogs.intervalStop = flowStaging.intervalStop "
                           "AND flowlogs.addressA = flowStaging.addressA "
                           "AND flowlogs.addressB = flowStaging.addressB "
                           "AND flowlogs.transportProtocol = flowStaging.transportProtocol "
                           "AND flowlogs.portA = flowStaging.portA "
                           "AND flowlogs.portB = flowStaging.portB "
                           "AND flowlogs.bytesAtoB = flowStaging.bytesAtoB "
                           "AND flowlogs.bytesBtoA = flowStaging.bytesBtoA);")
            self._cnx.commit()
        except Exception as e:
            self._cnx.rollback()
            raise e

    def flow_logs(self):
        """Provide an iterator over the main flow log."""
        return self._CursorIterator(connection=self._cnx,
                                    query="select * from flowlogs;")

    def stage_dns_logs(self):
        """Stage DNS log entries from the main log in a temporary table.

        This will clear and overwrite any currently staged logs with new logs
        from the main store.

        Returns an iterator over the newly staged logs.
        """
        self._cnx.start_transaction(isolation_level="SERIALIZABLE")
        try:
            cursor = self._cnx.cursor()
            cursor.execute("drop table if exists dnsStaging;"
                           "create table dnsStaging "
                           "as (select time, srcIp, dstIp, transportProtocol, "
                           "srcPort, dstPort, opcode, resultcode, host, "
                           "ip_addresses, ttls, idx "
                           "from dnsResponses, answers "
                           "where dnsResponses.answer=answers.idx);")

            self._cnx.commit()
        except Exception as e:
            self._cnx.rollback()
            raise e

        return self._CursorIterator(connection=self._cnx,
                                    query="select * from dnsStaging;")

    def purge_staged_dns_logs(self):
        """Delete any DNS entries from the main log which have been staged."""
        self._cnx.start_transaction(isolation_level="SERIALIZABLE")
        try:
            cursor = self._cnx.cursor()
            cursor.execute("DELETE FROM dnsResponses "
                           "WHERE EXISTS ( "
                           "SELECT time, srcIp, dstIp, transportProtocol, "
                           "srcPort, dstPort, opcode, resultcode, host, "
                           "ip_addresses, ttls, idx "
                           "FROM dnsStaging "
                           "WHERE dnsResponses.answer = dnsStaging.idx AND "
                           "dnsResponses.time = dnsStaging.time AND "
                           "dnsResponses.srcIp = dnsStaging.srcIp AND "
                           "dnsResponses.dstIp = dnsStaging.dstIp AND "
                           "dnsResponses.transportProtocol = dnsStaging.transportProtocol AND "
                           "dnsResponses.srcPort = dnsStaging.srcPort AND "
                           "dnsResponses.dstPort = dnsStaging.dstPort AND "
                           "dnsResponses.opcode = dnsStaging.opcode AND "
                           "dnsResponses.resultcode = dnsStaging.resultcode)"
                           )
            self._cnx.commit()
        except Exception as e:
            self._cnx.rollback()
            raise e

    def dns_logs(self):
        """Provide an iterator over the main DNS log."""
        query_string = "select time, srcIp, dstIp, transportProtocol, srcPort, dstPort, opcode, resultcode, host, ip_addresses, ttls, idx from dnsResponses, answers where dnsResponses.answer=answers.idx;"

        return self._CursorIterator(connection=self._cnx, query=query_string)

    def ip_imsi_table(self):
        """Provide an iterator over the IMSI<->IP mapping."""
        return self._CursorIterator(connection=self._cnx,
                                    query="select * from static_ips")
