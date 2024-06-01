from mrjob.job import MRJob
from mrjob.step import MRStep

class GenreRating(MRJob):

    def mapper(self, _, line):
        _, movie , rating , genre , _ = line.split(',')
        yield genre, (movie,float(rating))

    def reducer(self, genre , ratings):
        ratingsList = list(ratings)
        moviesRating = 0

        for _, rating in ratingsList:
            moviesRating += rating

        averageRating = moviesRating/len(ratingsList)

        yield averageRating, genre

    def combinereducer(self, averageRating, genre):
        yield None, (averageRating, list(genre))

    def maximumAndMinimum(self, _, data):
        dataList = list(data)
        yield ("min: ", min(dataList))
        yield ("max: ", max(dataList))

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.combinereducer),
            MRStep(reducer=self.maximumAndMinimum)
        ]
if __name__ == '__main__':
    GenreRating.run()