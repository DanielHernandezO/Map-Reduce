
from mrjob.job import MRJob

class UsersPerMovie(MRJob):

    def mapper(self, _, line):
        user, movie , rating , _ , _ = line.split(',')
        yield movie, (user,int(rating))

    def reducer(self, movie , ratings):
        raitingsList = list(ratings)
        usersRating = 0

        for _, rating in raitingsList:
            usersRating+=rating

        yield movie, ("users", len(raitingsList), usersRating/len(raitingsList))

if __name__ == '__main__':
    UsersPerMovie.run()