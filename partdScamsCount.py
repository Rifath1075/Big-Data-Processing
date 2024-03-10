from mrjob.job import MRJob
import re
import time
WORD_REGEX = re.compile(r"\b\w+\b")
class partDScamsCount(MRJob):
    def mapper(self, _, line):

        try:
            fields = line.split(',')
            category = fields[-1]
            yield (category, 1)
        except:
            pass

    def combiner(self, category, counts):
        #total = sum(counts)
        yield(category,sum(counts))

    def reducer(self, category, counts):
        total = sum(counts)
        yield(category,total)

if __name__ == "__main__":
    partDScamsCount.run()
