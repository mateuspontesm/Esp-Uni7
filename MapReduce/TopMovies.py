from mrjob.job import MRJob
from mrjob.step import MRStep


class TopMovies(MRJob):
    def mapper(self, _, line):
        data = line.split()
        yield data[1], 1

    def reducerMovie(self, data, values):
        sums = sum(values)
        yield sums, (sums, data)

    def mapper2(self, _, line):
        yield None, line

    def reducer2(self, _, values):
        sort = sorted(values, reverse=True)  # Sorting
        for i in range(10):  # Top Ten Movies
            yield i + 1, sort[i][1]

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducerMovie),
                MRStep(mapper=self.mapper2, reducer=self.reducer2)]


if __name__ == '__main__':
    TopMovies.run()
