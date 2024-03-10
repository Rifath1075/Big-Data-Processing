from mrjob.job import MRJob
import re
import time
WORD_REGEX = re.compile(r"\b\w+\b")
class partb3(MRJob):
    def mapper(self, _, line):
        try:
            fields = line.split('\t')
            address = fields[0]
            x = re.findall(r'"(.*?)"', address)
            address = x[0]
            value = int(fields[1])
            yield (1, (address,value))	
        except:
            pass

    def reducer(self, key, values):
        topten = []
        for i in values:
            topten.append(i)
            topten.sort(key=lambda x: x[1])
            topten = topten[-10:]
        for j in topten:
            yield(j[0],j[1])     

if __name__ == "__main__":
    partb3.run()
