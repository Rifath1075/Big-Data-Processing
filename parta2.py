from mrjob.job import MRJob
import re
import time
WORD_REGEX = re.compile(r"\b\w+\b")
class parta2(MRJob):
    def mapper(self, _, line):
        fields = line.split(',')
        try:
            if (len(fields)==7):
                timestamp = int(fields[6])
                month = time.strftime("%m",time.gmtime(timestamp))
                year = time.strftime("%y",time.gmtime(timestamp))
                yearmonth = year + month
                yield (yearmonth, int(fields[3]))
        except:
            pass

    def combiner(self, yearmonth, values):
        count = 0
        total = 0
        for i in values:
            count += 1
            total += i

        yield(yearmonth, (total,count))

    def reducer(self, yearmonth, values):
        count = 0
        total = 0
        for i in values:
            count += i[1]
            total += i[0]

        yield(yearmonth,total/count)

if __name__ == "__main__":
    parta2.run()
