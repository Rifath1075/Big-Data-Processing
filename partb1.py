from mrjob.job import MRJob
import re
import time
WORD_REGEX = re.compile(r"\b\w+\b")
class partb1(MRJob):
    def mapper(self, _, line):
        fields = line.split(',')
        try:
            if (len(fields)==7):
                address = fields[2]
                value = int(fields[3])
                yield (address, value)	
        except:
            pass

    def combiner(self, address, values):
        yield(address,sum(values))

    def reducer(self, address, values):
        yield(address,sum(values))

if __name__ == "__main__":
    partb1.run()
