# _*_ coding=utf-8 _*_
import json
import os
import sys
import time
import urllib2

# 从ElasticSearch导入数据和导出数据

reload(sys)
sys.setdefaultencoding('utf-8')  # @UndefinedVariable

class exportEsData():
    size = 10000

    def __init__(self, url, index, type):
        self.url = url + "/" + index + "/" + type + "/_search"
        self.index = index
        self.type = type

    def exportData(self):
        print("export data begin...")
        begin = time.time()
        # try:
        #     os.remove(self.index + "_" + self.type + ".json")
        # except:
        #     print self.index + "_" + self.type + ".json"
        #     os.mknod(self.index + "_" + self.type + ".json")
        msg = urllib2.urlopen(self.url).read()
        print(msg)
        obj = json.loads(msg)
        num = obj["hits"]["total"]
        start = 0
        end = num / self.size + 1
        i = 0
        while (start < end):
            msg = urllib2.urlopen(self.url + "?from=" + str(start * self.size) + "&size=" + str(self.size)).read()
            self.writeFile(msg,start)
            start = start + 1


        print("export data end!!!\n\t total consuming time:" + str(time.time() - begin) + "s")

    def writeFile(self, msg,i):
        obj = json.loads(msg)
        vals = obj["hits"]["hits"]
        try:
            f = open(self.index + "_" + self.type + "_"+str(i)+".json", "a")
            print self.index + "_" + self.type + "_"+str(i)+".json"
            for val in vals:
                a = json.dumps(val["_source"], ensure_ascii=False)
                f.write(a + "\n")
        finally:
            f.flush()
            f.close()


class importEsData():
    def __init__(self, url, index, type):
        self.url = url + "/" + index + "/" + type
        self.index = index
        self.type = type

    def importData(self):
        print("import data begin...")
        begin = time.time()
        try:
            f = open(self.index + "_" + self.type + ".json", "r")
            for line in f:
                self.post(line)
        finally:
            f.close()
        print("import data end!!!\n\t total consuming time:" + str(time.time() - begin) + "s")

    def post(self, data):
        req = urllib2.Request(self.url, data, {"Content-Type": "application/json; charset=UTF-8"})
        urllib2.urlopen(req)


if __name__ == '__main__':
    '''
        Export Data
        e.g.
                            URL                    index        type
        exportEsData("http://10.100.142.60:9200","watchdog","mexception").exportData()
    '''
    # exportEsData("http://10.100.142.60:9200","watchdog","mexception").exportData()
    exportEsData("http://10.200.50.230:9200", "idea.question.v1", "question").exportData()

    '''
        Import Data
        *import file name:watchdog_test.json    (important)
                    "_" front part represents the elasticsearch index
                    "_" after part represents the  elasticsearch type
        e.g.
                            URL                    index        type
        mportEsData("http://10.100.142.60:9200","watchdog","test").importData()
    '''
    # importEsData("http://10.100.142.60:9200","watchdog","test").importData()
    # importEsData("http://10.100.142.60:9200", "watchdog", "test").importData()