from collections import OrderedDict 
import pickle
import matplotlib.pyplot as plt
import numpy as np

Path = "./Data/"
Trials = 15
Time = [10, 20, 30, 40]

FolderName = ["10-min", "20-min", "30-min", "40-min"]

def readFile(name):
    return pickle.load(open(name, 'rb'))

def readTweetCounts(name):
    return pickle.load(open(name, 'rb'))

def AMS():
    surprise_nums = {10: [], 20: [], 30: [], 40: []}
    TweetCount = readTweetCounts("./Data/TweetCount.pkl")
    for i in range(len(FolderName)):
        for j in range(Trials):
            tweets = readFile(Path + FolderName[i] + "/tweet_" + str(j+1))
            values = list(tweets.values()) 
            sum_values = TweetCount[Time[i]][j]
            # sum_values = sum(values)
            print("Sum_Values:", sum_values)
            denom = len(tweets)
            num = 0
            for val in values:
                num += 2*val - 1
            num = num*sum_values
            surprise_number = num/denom
            surprise_nums[Time[i]].append(surprise_number)
            print(surprise_number, end = " ")
        save_graph_time(surprise_nums[Time[i]], i)
        print()
    save_graph(surprise_nums)

def save_graph_time(surprise_num, interval):
    plt.figure()
    plt.plot(np.arange(Trials), surprise_num, label=FolderName[interval])
    plt.xlabel("Trials")
    plt.ylabel("Surprise Number")
    plt.legend()
    plt.title("Surprise number behavior for " + FolderName[interval])
    plt.savefig('./Graphs/' + str(interval) + '.png')
    plt.close()


def save_graph(surprise_num):
    plt.figure()
    for i in range(len(Time)):
        plt.plot(np.arange(Trials), surprise_num[Time[i]], label=FolderName[i])
    plt.xlabel("Trials")
    plt.ylabel("Surprise Number")
    plt.legend()
    plt.title("Surprise number behavior for all time interval")
    plt.savefig('./Graphs/secondMoment.png')
    plt.close()

AMS()