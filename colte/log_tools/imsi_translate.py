import sys
import hashlib

# Michael Fang 2/23

# A few things to clarify:
#    we choose the string of the input key to be the seed
#    we hash the IMSI by treating the number as a string appended with seed
#    a few more things to notice in the comments among the codes


def code_imsi(imsi, key):
    """Code the imsi in an (ideally) irreversable manner"""
    return hashlib.sha256(imsi + key).hexdigest()


def _translate_line(words, seed):
    """Code any IMSI encountered on the line

    @returns coded_result: the line with IMSIs replaced with encoded imsis
    """
    coded_result = ""
    # we assume string starts with 46066 and len(str) is 15 is imsi value
    # we assume one space between two fields in log file
    for word in words:
        if ((len(word) == 15) and word.startswith("90154")):
            # hash the imsi value
            word = code_imsi(word, seed)
        coded_result += word + " "
    return coded_result


def _print_usage():
    sys.exit("Usage: IMSI_Translate.py input_log_file output_log_file Key")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        _print_usage()

    try:
        f_input = open(sys.argv[1], "r")
        f_output = open(sys.argv[2], "w")
        key = sys.argv[3]
    except IndexError:
        _print_usage()

    # hash imsi and put result in output file
    for line in f_input:
        result = _translate_line(line.split(), key)
        f_output.write(result + "\n")
