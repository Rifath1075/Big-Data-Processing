from mrjob.job import MRJob
import re
import time
WORD_REGEX = re.compile(r"\b\w+\b")
class partc(MRJob):
    def mapper(self, _, line):
        try:
            fields = line.split(',')
            address = fields[2]
            size = int(fields[4])
            dic = {address: size}
            yield (address, size)
        except:
            pass

    def combiner(self, address, size):
        yield(address, sum(size))

    def reducer(self, address, size):
        #topten = []
        #for i in values:
        #    total = sum(size)
        #    dic = {address: total}
        #topten.append(dict(dic))
        #topten.sort(reverse = True, key=lambda x: x[1])
        #topten = topten[:10]
        #for j in topten:
        #    yield(j)
        yield(address, sum(size))

    #combiner = reducer


if __name__ == "__main__":
    partc.run()
