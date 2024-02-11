from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingAvg(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    def mapper(self, _, line):
        separate = line.split(",")
        if len(separate) == 4 and separate[2].replace('.', '', 1).isdigit():
            movie_id = int(separate[1])
            rating = float(separate[2])
            yield movie_id, rating

    def reducer(self, movie_id, ratings):
        ratings_list = list(ratings)
        if ratings_list:
            avg_rating = sum(ratings_list) / len(ratings_list)
            yield movie_id, avg_rating

if __name__ == '__main__':
    RatingAvg.run()
