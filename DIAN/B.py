from mrjob.job import MRJob

class AverageSalaryPerEmployee(MRJob):

    def mapper(self, _, line):
        idemp, _, salary, _ = line.split(',')
        yield idemp, int(salary)

    def reducer(self, idemp, salaries):
        salariesList = list(salaries)

        average = sum(salariesList) / len(salariesList)

        yield idemp, average

if __name__ == '__main__':
    AverageSalaryPerEmployee.run()