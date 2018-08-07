from mrjob.job import MRJob
from mrjob.step import MRStep


class MovieRating(MRJob):
    def mapper(self, _, line):
        words = line.split()
        yield words[1], int(words[2])

    def reducerMovie(self, word, values):
        sums = 0
        count = 0
        for value in values:
            sums += value
            count += 1
        yield sums / count, word

    def reducerSort(self, word, values):
        yield word, values

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducerMovie), MRStep(reducer=self.reducerSort)]


if __name__ == '__main__':
    MovieRating.run()
