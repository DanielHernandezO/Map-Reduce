from mrjob.job import MRJob
from mrjob.step import MRStep

class MinimumMoviesInADay(MRJob):

    def mapper(self, _, line):
        _, movie , _ , _ , date = line.split(',')
        yield date, movie

    def reducer(self, date , movies):
        moviesList = list(movies)

        yield len(moviesList),date

    def combinereducer(self, quantity, dates):
        yield None, (quantity, list(dates))

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
    MinimumMoviesInADay.run()