from mrjob.job import MRJob
import re
import time
WORD_REGEX = re.compile(r"\b\w+\b")
class partb2(MRJob):
    def mapper(self, _, line):
        try:
            if (len(line.split('\t'))==2): #agg.txt
                fields = line.split('\t')
                address = fields[0]
                x = re.findall(r'"(.*?)"', address)
                address = x[0]
                value = int(fields[1])
                if (value==0):
                    pass
                else:
                    yield (address, (value,1))	

            elif (len(line.split(','))==5): #contracts.csv
                fields=line.split(',')
                address=fields[0]
                yield (address,(1,2))

        except:
            pass

    def reducer(self, address, values):
        val = []
        add = False

        for i in values:
            if i[1] == 1:
                val.append(i[0])
            elif i[1] == 2:
                add = True

        if(add and sum(val)!=0):
            yield(address, sum(val))

if __name__ == "__main__":
    partb2.run()
