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

    class _FlowLogIterator(object):
        def __init__(self, connection):
            self._connection = connection

        def __enter__(self):
            self._cursor = self._connection.cursor()
            query = ("select * from flowlogs;")
            self._cursor.execute(query)
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self._cursor.close()

        def __iter__(self):
            return self

        def __next__(self):
            return self._cursor.__next__()

    def flow_logs(self):
        return self._FlowLogIterator(self._cnx)

    class _DnsLogIterator(object):
        def __init__(self, connection):
            self._connection = connection

        def __enter__(self):
            self._cursor = self._connection.cursor()
            query = ("select time, srcIp, dstIp, transportProtocol, srcPort, dstPort, opcode, resultcode, host, ip_addresses, ttls, idx from dnsResponses, answers where dnsResponses.answer=answers.idx;")
            self._cursor.execute(query)
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self._cursor.close()

        def __iter__(self):
            return self

        def __next__(self):
            return self._cursor.__next__()

    def dns_logs(self):
        return self._DnsLogIterator(self._cnx)

    class _IpToIMSIIterator(object):
        def __init__(self, connection):
            self._connection = connection

        def __enter__(self):
            self._cursor = self._connection.cursor()
            query = ("select * from static_ips")
            self._cursor.execute(query)
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self._cursor.close()

        def __iter__(self):
            return self

        def __next__(self):
            return self._cursor.__next__()

    def ip_imsi_table(self):
        return self._IpToIMSIIterator(self._cnx)
