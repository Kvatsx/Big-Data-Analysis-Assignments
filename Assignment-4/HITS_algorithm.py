import findspark
findspark.init()
findspark.find()

from pyspark import SparkContext
import time


def read_txt(path_links, path_titles):
    # Read
    links = sc.textFile(path_links)
    titles = sc.textFile(path_titles)

    # Split links
    links_formatted = links.map(lambda x: (int(x.split(':')[0]),
        [int(y) for y in x.split(':')[1].split(' ') if y != '']))

    # Zip titles
    titles_indexed = titles.zipWithIndex().map(lambda x: (x[1] + 1, x[0]))
    print(titles_indexed)
    titles_indexed.cache()

    return links_formatted, titles_indexed


def remove_dead_links(links_rdd):
    links_rdd.repartition(32)
    links_rdd.cache()
    print ("REMOVING DEAD INKS")
    out_links = links_rdd.flatMapValues(lambda x: [y for y in x]) \
                    .filter(lambda x: x[1] != [])
    print("REMOVING LINKS - DONE")
    out_links.cache()
    return out_links

def create_auths_hubs(titles_indexed):
    auths = titles_indexed.map(lambda x: (x[0], 1.0))
    print (auths.take(10))
    print ('\nAuths RDD created\n')
    hubs = auths
    print (hubs.take(10))
    print ('\nHubs RDD created\n')
    return auths, hubs

# UPDATE AUTHS
def update_auths(hubs):
    auths = out_links.join(hubs) \
            .map(lambda x: (x[1][0], x[1][1])) \
            .reduceByKey(lambda x, y: x + y)
    auths.cache()
    return auths

# UPDATE HUBS
def update_hubs(auths):
    hubs = out_links.map(lambda x: (x[1], x[0])) \
            .join(auths) \
            .map(lambda x: (x[1][0], x[1][1])) \
            .reduceByKey(lambda x, y: x + y)
    hubs.cache()
    return hubs

# NORMALIZE
def normalize(rdd):
    temp = rdd.map(lambda x: ('sum', x[1]**2)) \
            .reduceByKey(lambda x, y: x+y)
    norm = temp.collect()[0][1] ** 0.5
    updated = rdd.map(lambda x: (x[0], x[1] / norm))
    updated = updated.sortBy(lambda x: x[1], ascending=False)
    updated.cache()
    return updated

# CONVERT OUTPUT
def print_output(list_output, titles):
    index_titles = [x[0] for x in list_output]
    titles_output = titles.filter(lambda x: x[0] in index_titles).collect()
    print ('\nPAGE ID AND SCORE')
    for item in list_output:
        print (item)
    print ('\nPAGE ID AND TITLE')
    for item in titles_output:
        print (item)

# UPDATE PROCESS
def iterate_update(hubs, titles, n_iter):
    # In each iteration, update auths, update hubs and normalize both
    for i in range(n_iter):
        start = time.time()
        print ('===== ITERATION %s - UPDATING AUTHS =====' % (i + 1))
        auths = update_auths(hubs)
        print ('===== ITERATION %s - UPDATING HUBS =====' % (i + 1))
        hubs = update_hubs(auths)
        print ('===== ITERATION %s - NORMALIZING AUTHS =====' % (i + 1))
        auths = normalize(auths)
        print ('===== ITERATION %s - NORMALIZING HUBS =====' % (i + 1))
        hubs = normalize(hubs)
        elapsed = time.time() - start
        print ('Elapsed time: ', elapsed)
        if i in [0,7]:
            print ('===== ITERATION %s - AUTHS OUTPUT =====' % (i + 1))
            print_output(auths.takeOrdered(20, key=lambda x: -x[1]), titles)
            print ('===== ITERATION %s - HUBS OUTPUT =====' % (i + 1))
            print_output(hubs.takeOrdered(20, key=lambda x: -x[1]), titles)



if __name__ == "__main__":

    sc = SparkContext()

    # Read and format input
    path_links = "file:///D:\College\Semester-8\BDA\Big-Data-Analysis-Assignments\Assignment-4\\new_dataset.txt"
    path_titles = "file:///D:\College\Semester-8\BDA\Big-Data-Analysis-Assignments\Assignment-4\\titles-sorted.txt"
    links, titles = read_txt(path_links, path_titles)
    print ('TABLES CREATED')

    # Remove dead links
    out_links = remove_dead_links(links)

    # Create auths and hubs
    auths, hubs = create_auths_hubs(titles)

    # Iterate updating
    iterate_update(hubs, titles, 8)