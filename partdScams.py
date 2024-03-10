from mrjob.job import MRJob
import re
import time
WORD_REGEX = re.compile(r"\b\w+\b")
class partdScams(MRJob):
    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if (len(fields)==7): #transactions.csv
                address = fields[2]
                timestamp = int(fields[6])
                month = time.strftime("%m",time.gmtime(timestamp))
                year = time.strftime("%y",time.gmtime(timestamp))
                yearmonth = year + month
                yield(address, (yearmonth,1,1))

            elif (fields[2] == 'Offline' or fields[2] == 'Active'): #for scams2.csv, scams.csv would
            #use fields[-2] for the status. and fields[-1] for the category

                #yields the address, cateogry, and status
                yield(fields[1],(fields[3],fields[2],2))

        except:
            pass

    def reducer(self, address, values):
        category = []
        status = []
        yearmonth = []
        setT = False
        setS = False

        for i in values:
            if i[2] == 1:
                yearmonth.append(i[0])
                setT = True
            elif i[2] == 2:
                category.append(i[0])
                status.append(i[1])
                setS = True

        if(setT and setS):
            yield(yearmonth[0], address, status[0], category[0])

if __name__ == "__main__":
    partdScams.run()
