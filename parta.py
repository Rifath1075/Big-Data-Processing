from mrjob.job import MRJob
import re
import time
WORD_REGEX = re.compile(r"\b\w+\b")
class parta(MRJob):
    def mapper(self, _, line):
        fields = line.split(',')
        try:
            if (len(fields)==7):
                timestamp = int(fields[6])
                month = time.strftime("%m",time.gmtime(timestamp))   
                year = time.strftime("%y",time.gmtime(timestamp))
                yearmonth = year + month
                yield (yearmonth, 1)	
        except:
            pass

    def combiner(self, yearmonth, counts):
        #total = sum(counts)
        yield(yearmonth,sum(counts))

    def reducer(self, yearmonth, counts):
        total = sum(counts)
        yield(yearmonth,total)

if __name__ == "__main__":
    parta.run()
