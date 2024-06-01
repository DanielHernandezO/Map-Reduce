from mrjob.job import MRJob

class SectorPerEmployee(MRJob):

    def mapper(self, _, line):
        idemp, sececon, _, _ = line.split(',')
        yield idemp, sececon

    def reducer(self, idemp, sectors):
        yield idemp, len(list(sectors))

if __name__ == '__main__':
    SectorPerEmployee.run()