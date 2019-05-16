import argparse
import getpass
import lzma

import colte.db.reader
import colte.log_tools.encoder


PASS = None
FLOWLOG_OUT_FILE = "flowlog_archive"
DNS_OUT_FILE = "dns_archive"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--compress", action="store_true",
                        help="Compress the output archive files.")

    key_group = parser.add_mutually_exclusive_group(required=True)
    key_group.add_argument("-f", "--keyfile",
                           help="The file holding the encoding key.")
    key_group.add_argument("-k", "--key",
                           help="The key string to use when encoding data.")

    args = parser.parse_args()

    if not PASS or PASS is None:
        db_password = getpass.getpass(prompt="DB Password:")
    else:
        db_password = PASS

    reader = colte.db.reader.ColteReader(db_host="localhost",
                                         db_user="colte_db",
                                         db_name="colte_db",
                                         db_password=db_password,
                                         )

    if args.keyfile is not None:
        with open(args.keyfile, mode="rb") as f:
            key = f.read()
    else:
        key = args.key.encode('utf8')

    encoder = colte.log_tools.encoder.StreamingEncoder(reader, key)

    if args.compress:
        encoder.stream_flowlogs_to_file(FLOWLOG_OUT_FILE + ".xz",
                                        compressor=lzma.LZMACompressor())
        encoder.stream_dns_to_file(DNS_OUT_FILE + ".xz",
                                   compressor=lzma.LZMACompressor())
    else:
        encoder.stream_flowlogs_to_file(FLOWLOG_OUT_FILE, compressor=None)
        encoder.stream_dns_to_file(DNS_OUT_FILE, compressor=None)

    reader.close()
