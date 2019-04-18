import getpass
import ipaddress
import pickle
import mysql.connector
import sys

from colte.log_tools.imsi_translate import hash_imsi


PASS = None
FLOWLOG_OUT_FILE = "flowlog_archive"
DNS_OUT_FILE = "dns_archive"


def get_ip_to_imsis(connection):
    cursor = connection.cursor()
    query = ("select * from static_ips")
    cursor.execute(query)
    imsis = {}

    for imsi, ip_string in cursor:
        address = ipaddress.ip_address(ip_string)
        imsis[address] = imsi

    return imsis


def get_ip_to_obfuscated_id(connection, seed):
    ip_to_imsi = get_ip_to_imsis(connection)
    ip_to_id = {}
    for ip, imsi in ip_to_imsi.items():
        # TODO(matt9j) There is no defined encoding for the imsi in hash_imsi!
        ip_to_id[ip] = hash_imsi(imsi.encode('utf8'), seed)

    return ip_to_id


def store_flowlogs(connection, ip_to_id, filename):
    cursor = connection.cursor()
    query = ("select * from flowlogs;")

    cursor.execute(query)

    with open(filename, 'ab') as f:
        print("Beginning Flow Recording")
        i = 0
        for row in cursor:
            # Log Status
            if i % 10000 == 0:
                print("Reached row", i)
            i += 1

            row_fields = dict()
            row_fields["start_time"] = row[0]
            row_fields["end_time"] = row[1]
            if len(row[2]) == 16:
                address_a = ipaddress.IPv6Address(bytes(row[2]))
                address_b = ipaddress.IPv6Address(bytes(row[3]))
            elif len(row[2]) == 4:
                address_a = ipaddress.IPv4Address(bytes(row[2]))
                address_b = ipaddress.IPv4Address(bytes(row[3]))
            else:
                raise ValueError("IP length is invalid")

            # Handle Address Anonymization
            if address_a in ip_to_id.keys():
                row_fields["obfuscated_a"] = ip_to_id[address_a]
            else:
                row_fields["address_a"] = address_a

            if address_b in ip_to_id.keys():
                row_fields["obfuscated_b"] = ip_to_id[address_b]
            else:
                row_fields["address_b"] = address_b

            row_fields["transport_protocol"] = row[4]
            row_fields["port_a"] = row[5]
            row_fields["port_b"] = row[6]
            row_fields["bytes_a_to_b"] = row[7]
            row_fields["bytes_b_to_a"] = row[8]
            pickle.dump(row_fields, f)

    cursor.close()
    connection.close()


def store_dns(connection, ip_to_id, filename):
    cursor = connection.cursor()
    query = ("select time, srcIp, dstIp, transportProtocol, srcPort, dstPort, opcode, resultcode, host, ip_addresses, ttls, idx from dnsResponses, answers where dnsResponses.answer=answers.idx;")

    cursor.execute(query)

    with open(filename, 'ab') as f:
        print("Beginning DNS Recording")
        i = 0
        for row in cursor:
            # Log Status
            if i % 10000 == 0:
                print("Reached row", i)
            i += 1

            # Convert to ipaddress types
            if len(row[1]) == 4:
                src_addr = ipaddress.IPv4Address(bytes(row[1]))
            else:
                src_addr = ipaddress.IPv6Address(bytes(row[1]))

            if len(row[2]) == 4:
                dst_addr = ipaddress.IPv4Address(bytes(row[2]))
            else:
                dst_addr = ipaddress.IPv6Address(bytes(row[2]))

            row_fields = {"timestamp": row[0],
                          "src_ip": src_addr,
                          "dst_ip": dst_addr,
                          "protocol": row[3],
                          "src_port": row[4],
                          "dst_port": row[5],
                          "opcode": row[6],
                          "resultcode": row[7],
                          "host": row[8],
                          "response_addresses": list(),
                          "response_ttls": list(),
                          "answer_index": row[11],
                          }

            # Parse the variable number of response addresses and ttls
            addresses = row[9].split(",")
            ttls = row[10].split(",")
            for address, ttl in zip(addresses, ttls):
                if address != '':
                    if ttl == '':
                        raise ValueError("Mismatched number of address and ttl")
                    row_fields["response_addresses"].append(
                        ipaddress.ip_address(address))
                    row_fields["response_ttls"].append(ttl)

            # Obfuscate any local addresses:
            if src_addr in ip_to_id.keys():
                row_fields["obfuscated_src"] = ip_to_id[src_addr]
                del row_fields["src_ip"]

            if dst_addr in ip_to_id.keys():
                row_fields["obfuscated_dst"] = ip_to_id[dst_addr]
                del row_fields["dst_ip"]

            pickle.dump(row_fields, f)

    cursor.close()
    connection.close()


if __name__ == "__main__":

    if not PASS or PASS is None:
        db_password = getpass.getpass(prompt="DB Password:")
    else:
        db_password = PASS

    cnx = mysql.connector.connect(
        host="localhost",
        user="colte_db",
        database="colte_db",
        passwd=db_password
    )

    seed = sys.argv[1]

    ip_to_imsi = get_ip_to_imsis(cnx)
    ip_to_id = get_ip_to_obfuscated_id(cnx, seed.encode('utf8'))
    store_flowlogs(cnx, ip_to_id, FLOWLOG_OUT_FILE)
    store_dns(cnx, ip_to_id, DNS_OUT_FILE)
    print(ip_to_id)
