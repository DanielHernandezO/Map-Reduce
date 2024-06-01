from mrjob.job import MRJob
from mrjob.step import MRStep

class BestRating(MRJob):

    def mapper(self, _, line):
        _, _ , rating , _ , date = line.split(',')
        yield date, float(rating)

    def reducer(self, date , ratings):
        ratingsList = list(ratings)
        averageRating = sum(ratingsList)/len(ratingsList)
        yield averageRating, date

    def combinereducer(self, averageRating, dates):
        yield None, (averageRating, list(dates))

    def maximum(self, _, data):
        yield max(data)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.combinereducer),
            MRStep(reducer=self.maximum)
        ]
if __name__ == '__main__':
    BestRating.run()