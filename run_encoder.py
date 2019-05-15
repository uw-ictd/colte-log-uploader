import getpass
import sys

import colte.db.reader
import colte.log_tools.encoder


PASS = None
FLOWLOG_OUT_FILE = "flowlog_archive"
DNS_OUT_FILE = "dns_archive"


if __name__ == "__main__":
    if not PASS or PASS is None:
        db_password = getpass.getpass(prompt="DB Password:")
    else:
        db_password = PASS

    seed = sys.argv[1]

    reader = colte.db.reader.ColteReader(db_host="localhost",
                                         db_user="colte_db",
                                         db_name="colte_db",
                                         db_password=db_password,
                                         )

    encoder = colte.log_tools.encoder.StreamingEncoder(reader,
                                                       seed.encode('utf8'))
    encoder.stream_flowlogs_to_file(FLOWLOG_OUT_FILE)
    encoder.stream_dns_to_file(DNS_OUT_FILE)
    reader.close()
