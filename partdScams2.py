from mrjob.job import MRJob
import re
import time
WORD_REGEX = re.compile(r"\b\w+\b")
class partDScams2(MRJob):
    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if (fields[-2] == 'Offline' or fields[-2] == 'Active'):
                status = fields[-2]
                category = fields[-1]
                for i in range(1,len(fields)-2):
                    yield(fields[i],(category, status, 1))
            if (len(fields)==7):
                address = fields[2]
                timestamp = int(fields[6])
                month = time.strftime("%m",time.gmtime(timestamp))
                year = time.strftime("%y",time.gmtime(timestamp))
                yearmonth = year + month
                yield(address, (yearmonth,2,2))
        except:
            pass

    def reducer(self, address, values):
        category = None
        status = None
        yearmonth = None

        for i in values:
            if values[2]==1:
                category = str(values[0])
                status = str(values[1])
            if values[2]==2:
                yearmonth = values[0]

        if (category!=None and yearmonth!=None):
            yield(address,(yearmonth, category, status))



if __name__ == "__main__":
    partDScams2.run()
