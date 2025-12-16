import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: count_words_in_line <input_path>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("count-words-in-line")
    sc = SparkContext(conf=conf)

    text_file = sc.textFile("s3a://" + sys.argv[1])

    # map each line to (line, number of words in the line)
    line_word_counts = text_file.map(
        lambda line: (line, len(line.split()))
    )

    # reduce to find the line with the most number of words
    longest_line = line_word_counts.reduce(
        lambda a, b: a if a[1] >= b[1] else b
    )

    print("--------------------------------------------")
    print("Line with the most words:")
    print(longest_line[0])
    print("\nNumber of words:", longest_line[1])
    print("--------------------------------------------")
