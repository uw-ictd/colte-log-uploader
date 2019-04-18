import sys
import hashlib

# Michael Fang 2/23

# A few things to clarify:
#    we choose the string of the input key to be the seed
#    we hash the IMSI by treating the number as a string appended with seed
#    a few more things to notice in the comments among the codes


# Is it okay to use sha256?
# hash imsi
def hash_imsi(imsi, seed):
    return hashlib.sha256(imsi + seed).hexdigest()

def translate_line(words, seed):
    result = ""
    # we assume string starts with 46066 and len(str) is 15 is imsi value
    # we assume one space between two fields in log file
    for word in words:
        if ((len(word) == 15) and word.startswith("90154")):
            # hash the imsi value
            word = hash_imsi(word, seed)
        result += word + " "
    return result

def Usage():
    sys.exit("Usage: IMSI_Translate.py input_log_file output_log_file Key")

if __name__ == "__main__":
    if (len(sys.argv) != 4):
        Usage()

    try:
        f_input = open(sys.argv[1], "r")
        f_output = open(sys.argv[2], "w")  # "w" overwrite a file, change to "a" if append
        key = sys.argv[3]
    except:
        Usage()

    # hash imsi and put result in output file
    for line in f_input:
        result = translate_line(line.split(), key)
        f_output.write(result + "\n")
