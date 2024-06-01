from mrjob.job import MRJob

class MoviesPerUser(MRJob):

    def mapper(self, _, line):
        user, _ , rating, _ , _ = line.split(',')
        yield user, float(rating)

    def reducer(self, user, movies):
        moviesList = list(movies)

        average = sum(moviesList) / len(moviesList)

        yield user, ("movies: ",len(moviesList), "rating: ", average)

if __name__ == '__main__':
    MoviesPerUser.run()