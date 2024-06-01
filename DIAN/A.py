from mrjob.job import MRJob

class AverageSalaryPerSececon(MRJob):

    def mapper(self, _, line):
        _, sececon, salary, _ = line.split(',')
        yield sececon, int(salary)

    def reducer(self, sececon, salaries):
        salariesList = list(salaries)

        average = sum(salariesList) / len(salariesList)

        yield sececon, average

if __name__ == '__main__':
    AverageSalaryPerSececon.run()