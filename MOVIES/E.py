from mrjob.job import MRJob
from mrjob.step import MRStep

class WorstRating(MRJob):

    def mapper(self, _, line):
        _, _ , rating , _ , date = line.split(',')
        yield date, float(rating)

    def reducer(self, date , ratings):
        ratingsList = list(ratings)
        averageRating = sum(ratingsList)/len(ratingsList)
        yield averageRating, date

    def combinereducer(self, averageRating, dates):
        yield None, (averageRating, list(dates))

    def minimum(self, _, data):
        yield min(data)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.combinereducer),
            MRStep(reducer=self.minimum)
        ]
if __name__ == '__main__':
    WorstRating.run()