import ipaddress
import pickle

from colte.log_tools.imsi_translate import code_imsi


class StreamingEncoder(object):
    """Encodes CoLTE data for archival and export.

    Provides methods for reading data and streaming it to both compressed and
    uncompressed archive files.
    """
    def __init__(self, reader, seed):
        self._reader = reader
        self._coded_ids = self._build_ip_to_coded_id(seed)

    def _build_ip_to_imsis(self):
        """Build and return a dictionary mapping IP addresses to IMSIs."""
        imsis = {}
        with self._reader.ip_imsi_table() as imsi_table:
            for imsi, ip_string in imsi_table:
                address = ipaddress.ip_address(ip_string)
                imsis[address] = imsi
        return imsis

    def _build_ip_to_coded_id(self, seed):
        """Build and return a dict mapping IP addresses to anonymized ID."""
        ip_to_imsi = self._build_ip_to_imsis()
        ip_to_id = {}
        for ip, imsi in ip_to_imsi.items():
            ip_to_id[ip] = code_imsi(imsi, seed)

        return ip_to_id

    def _encode_flowlog(self, flowlog):
        """Transcribe a flowlog tuple into a dict for output and archival."""
        row_fields = dict()
        row_fields["start_time"] = flowlog[0]
        row_fields["end_time"] = flowlog[1]
        if len(flowlog[2]) == 16:
            address_a = ipaddress.IPv6Address(bytes(flowlog[2]))
            address_b = ipaddress.IPv6Address(bytes(flowlog[3]))
        elif len(flowlog[2]) == 4:
            address_a = ipaddress.IPv4Address(bytes(flowlog[2]))
            address_b = ipaddress.IPv4Address(bytes(flowlog[3]))
        else:
            raise ValueError("IP length is invalid")

        # Handle Address Anonymization
        if address_a in self._coded_ids.keys():
            row_fields["obfuscated_a"] = self._coded_ids[address_a]
        else:
            row_fields["address_a"] = address_a

        if address_b in self._coded_ids.keys():
            row_fields["obfuscated_b"] = self._coded_ids[address_b]
        else:
            row_fields["address_b"] = address_b

        row_fields["transport_protocol"] = flowlog[4]
        row_fields["port_a"] = flowlog[5]
        row_fields["port_b"] = flowlog[6]
        row_fields["bytes_a_to_b"] = flowlog[7]
        row_fields["bytes_b_to_a"] = flowlog[8]

        return row_fields

    def _encode_dns(self, raw_log):
        """Transcribe a dns tuple into a dict for output and archival."""
        # Convert to ipaddress types
        if len(raw_log[1]) == 4:
            src_addr = ipaddress.IPv4Address(bytes(raw_log[1]))
        else:
            src_addr = ipaddress.IPv6Address(bytes(raw_log[1]))

        if len(raw_log[2]) == 4:
            dst_addr = ipaddress.IPv4Address(bytes(raw_log[2]))
        else:
            dst_addr = ipaddress.IPv6Address(bytes(raw_log[2]))

        row_fields = {"timestamp": raw_log[0],
                      "src_ip": src_addr,
                      "dst_ip": dst_addr,
                      "protocol": raw_log[3],
                      "src_port": raw_log[4],
                      "dst_port": raw_log[5],
                      "opcode": raw_log[6],
                      "resultcode": raw_log[7],
                      "host": raw_log[8],
                      "response_addresses": list(),
                      "response_ttls": list(),
                      "answer_index": raw_log[11],
                      }

        # Parse the variable number of response addresses and ttls
        addresses = raw_log[9].split(",")
        ttls = raw_log[10].split(",")
        for address, ttl in zip(addresses, ttls):
            if address != '':
                if ttl == '':
                    raise ValueError("Mismatched number of address and ttl")
                row_fields["response_addresses"].append(
                    ipaddress.ip_address(address))
                row_fields["response_ttls"].append(ttl)

        # Obfuscate any local addresses:
        if src_addr in self._coded_ids.keys():
            row_fields["obfuscated_src"] = self._coded_ids[src_addr]
            del row_fields["src_ip"]

        if dst_addr in self._coded_ids.keys():
            row_fields["obfuscated_dst"] = self._coded_ids[dst_addr]
            del row_fields["dst_ip"]

        return row_fields

    @staticmethod
    def _stream_to_file(filename, compressor, source, encode):
        """Stream an iterator source through an encoder to the log file."""
        with open(filename, 'wb') as f:
            print("Beginning", filename)

            for i, row in enumerate(source):
                # Log Status
                if i % 10000 == 0:
                    print("Reached row", i)

                encoded_log = encode(row)
                out_data = pickle.dumps(encoded_log)

                if compressor is not None:
                    out_data = compressor.compress(out_data)

                f.write(out_data)

            # Flush the incremental compressor after processing all rows.
            if compressor is not None:
                f.write(compressor.flush())

    def stream_flowlogs_to_file(self, filename, compressor=None):
        """Stream flow logs to an output file."""
        with self._reader.stage_flow_logs() as flow_logs:
            self._stream_to_file(filename, compressor, flow_logs,
                                 self._encode_flowlog)

    def stream_dns_to_file(self, filename, compressor=None):
        """Stream DNS logs to an output file."""
        with self._reader.stage_dns_logs() as dns_logs:
            self._stream_to_file(filename, compressor, dns_logs,
                                 self._encode_dns)

    # TODO(matt9j) The concept of staging doesn't exist at this higher API
    #  level. Consider how to best communicate the intention and encourage
    #  appropriate API use.
    def purge_staged_dns_logs(self):
        """Purge staged DNS logs from the main log."""
        self._reader.purge_staged_dns_logs()

    def purge_staged_flow_logs(self):
        """Purge staged flow logs from the main log."""
        self._reader.purge_staged_flow_logs()
