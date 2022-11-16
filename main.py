

# Import necessary submodules from PySpark package


import pyspark
from pyspark import SparkContext

def wordCount(wordListRDD):
    """Creates a pair RDD with word counts
    Args:
        wordListRDD (RDD of str): An RDD consisting of words.

    Returns:
        RDD of (str, int): An RDD consisting of (word, count) tuples.
    """
    #<FILL IN>
    return wordListRDD.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)


if __name__ == "__main__":
    # Creating 'stop_words' list 
    # Creating spark context named 'sc'
    sc =SparkContext()
    file_path = "shakespeare.txt"

    # read data and convert to baseRDD
    #  using 'textFile()' function 
    baseRDD = sc.textFile(file_path)

    # Creating new RDD from 'baseRDD' named 'splitRDD'
    splitRDD = baseRDD.flatMap(lambda x: x.split(" "))

    # count word split 
    wordsplitRDD = splitRDD.map(lambda x: (x, 1))
    wordCounts = wordsplitRDD.reduceByKey(lambda x,y : x+y)
    
    

    # find stop words 
    stopwordcounts25 = wordCount(splitRDD).sortBy(lambda x: x[1], ascending=False).take(25) 
    stop_words = [i[0] for i in stopwordcounts25]
    # print(stop_words)
     # convert to lowercase from splitRDD and remove stopword from file 
    # link: https://gist.github.com/sebleier/554280 
    # stop_words = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 
    #  'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 
    #  'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them',
    #   'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 
    #   'that', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 
    #   'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 
    #   'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 
    #   'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 
    #   'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 
    #   'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 
    #           'very', 'can', 'will', 'just', 'don', 'should', 'now','']

    splitRDD_filter =  splitRDD.filter(lambda word: word.lower() not in stop_words )

    # Create PairRDD tuple with value 1 
    PairRDD_tuple = splitRDD_filter.map(lambda w: (w, 1))

    reducedRDD = PairRDD_tuple.reduceByKey(lambda x,y: x + y)
    
    # show 10 word 
    #print(reducedRDD.take(10))

    # Hoan doi key va value 
    swappedRDD = reducedRDD.map(lambda word: (word[1], word[0]))
    resultRDD = swappedRDD.sortByKey(ascending=False)

    print(resultRDD.take(50))