import findspark
findspark.init()
findspark.find()
from pyspark import SparkContext

# Code reference - https://raw.githubusercontent.com/jaimeps/hits-algorithm/master/spark/hits_spark.py

def ConvertDataset():
    dataset = open('soc-Epinions1.txt', 'r')
    lines = dataset.readlines()
    lines = lines[4:] # skipping the comments in file

    graph = {}
    for line in lines:
        source, dest = line.split()
        if not source in graph:
            graph[source] = []
        graph[source].append(dest)
    
    new_dataset_obj = open('new_dataset.txt', 'w+')
    for key in graph:
        line = str(key) + ": " + " ".join(graph[key])
        new_dataset_obj.write(line + "\n")
    new_dataset_obj.close()

    writer = open("titles-sorted.txt", 'w+')
    keysList = list(graph.keys())
    keysList = [int(x) for x in keysList]
    keysList.sort()
    pointer = 1
    for i in range(1, max(keysList)):
        if (keysList[pointer] == i):
            pointer += 1
            writer.write(str(i) + "\n")
        else:
            writer.write(" \n")
    writer.close()
    print("DATA CONVERSION DONE!")

def read_txt(path_links, path_titles):
    # Read
    links = sc.textFile(path_links)
    titles = sc.textFile(path_titles)

    # Split links
    links_formatted = links.map(lambda x: (int(x.split(':')[0]), [int(y) for y in x.split(':')[1].split(' ') if y != '']))

    # Zip titles
    titles_indexed = titles.zipWithIndex().map(lambda x: (x[1] + 1, x[0]))
    titles_indexed.cache()

    return links_formatted, titles_indexed


def remove_dead_links(links_rdd):
    links_rdd.repartition(32)
    links_rdd.cache()
    out_links = links_rdd.flatMapValues(lambda x: [y for y in x]).filter(lambda x: x[1] != [])
    out_links.cache()
    print("DEAD LINKS REMOVED!")
    return out_links

def create_auths_hubs(titles_indexed):
    auths = titles_indexed.map(lambda x: (x[0], 1.0))
    print (auths.take(10))
    print ('\nAuths RDD created\n')
    hubs = auths
    print (hubs.take(10))
    print ('\nHubs RDD created\n')
    return auths, hubs

def update_auths(hubs):
    auths = out_links.join(hubs).map(lambda x: (x[1][0], x[1][1])).reduceByKey(lambda x, y: x + y)
    auths.cache()
    return auths

def update_hubs(auths):
    hubs = out_links.map(lambda x: (x[1], x[0])).join(auths).map(lambda x: (x[1][0], x[1][1])).reduceByKey(lambda x, y: x + y)
    hubs.cache()
    return hubs

def normalize(rdd):
    temp = rdd.map(lambda x: ('sum', x[1]**2)).reduceByKey(lambda x, y: x+y)
    norm = temp.collect()[0][1] ** 0.5
    updated = rdd.map(lambda x: (x[0], x[1] / norm))
    updated = updated.sortBy(lambda x: x[1], ascending=False)
    updated.cache()
    return updated

def print_output(list_output):
    index_titles = [x[0] for x in list_output]
    print ('\nPAGE ID AND SCORE')
    for item in list_output:
        print (item)

def iterate_update(hubs, n_iter):
    # In each iteration, update auths, update hubs and normalize both
    for i in range(n_iter):
        print("ITERATION {}".format(i+1))
        print('UPDATING AUTHS')
        auths = update_auths(hubs)
        print ('UPDATING HUBS')
        hubs = update_hubs(auths)
        print ('NORMALIZING AUTHS')
        auths = normalize(auths)
        print ('NORMALIZING HUBS')
        hubs = normalize(hubs)
        print ('AUTHS OUTPUT')
        print_output(auths.takeOrdered(20, key=lambda x: -x[1]))
        print ('HUBS OUTPUT')
        print_output(hubs.takeOrdered(20, key=lambda x: -x[1]))



if __name__ == "__main__":

    sc = SparkContext()
    ConvertDataset()
    # Read and format input
    links, titles = read_txt("new_dataset.txt", "titles-sorted.txt")
    print ('TABLES CREATED')
    out_links = remove_dead_links(links)
    auths, hubs = create_auths_hubs(titles)
    iterate_update(hubs, 10)