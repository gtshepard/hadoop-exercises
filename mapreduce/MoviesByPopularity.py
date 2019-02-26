from mrjob.job import MRJob
from mrjob.step import MRStep
# exercise for mapreduce from udemy course: The Ultimate Hands on Hadoop by frank kane 
# Sorts movies by popularity from grouplens ml-100k data set 
class MoviesByPopularity(MRJob):
    def steps(self):
        return [ MRStep(mapper=self.mapper_get_movies,
                   reducer=self.reducer_count_movies),
                 MRStep(reducer=self.reducer_movie_by_popularity)]

    # seperates data into key, value pairs as the data comes in (line by line)
    # the movieID is the  key, we assign a 1 as a value to each movie id we see, becuase
    # this means the movie has been rated. 
    def mapper_get_movies(self, _, data_line):
        (userID, movieID, rating, timestamp) = data_line.split('\t')
        yield movieID, 1

    # after mapreduce internally does shuffle and sort(basicaly combines keys
    # with same name), our reducer sums all the values for a given key to produce 
    # the number of times this movie has been rated. (how popular it is)
    def reducer_count_movies(self, key, values):
        yield str(sum(values)).zfill(5), key 

    # this reducer fucntion  sorts the movies by id and shows there popularity 
    def reducer_movie_by_popularity(self, count, movies):
        for movie in movies:
            yield movie, count


if __name__ == '__main__':
    #starts the mapreduce job
    MoviesByPopularity.run()
