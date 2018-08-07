from mrjob.job import MRJob
from mrjob.step import MRStep


class PopularHero(MRJob):
    def mapper1st(self, _, line):
        data = line.split()
        yield data[0], len(data[1:])

    def reducerSum(self, data, values):
        yield data, (sum(values), data)

    def mapper2nd(self, _, line):
        yield None, line

    def reducerMax(self, _, values):
        maximum = max(values)
        yield maximum[1], maximum[0]

    def steps(self):
        return [MRStep(mapper=self.mapper1st, reducer=self.reducerSum),
                MRStep(mapper=self.mapper2nd, reducer=self.reducerMax)]


if __name__ == '__main__':
    PopularHero.run()
