from mrjob.job import MRJob
from mrjob.step import MRStep

class BlackDay(MRJob):

    def mapper(self, _, line):
        company, price, date = line.split(',')
        yield company, (float(price), date)

    def reduceFindMinPrice(self, _, values):
        minDate = min(values)
        yield minDate[1], 1

    def reducerFindBlackDay(self, date, counts):
        yield None, (sum(counts), date)

    def reducerFindMaxBlackDay(self, _, data):
        yield max(data)

    def steps(self):
        return [
            MRStep(mapper=self.mapper, 
                   reducer=self.reduceFindMinPrice),
            MRStep(reducer=self.reducerFindBlackDay),
            MRStep(reducer=self.reducerFindMaxBlackDay)
        ]

if __name__ == '__main__':
    BlackDay.run()