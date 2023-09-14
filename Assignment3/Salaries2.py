from mrjob.job import MRJob

class MRSalaries(MRJob):

    def categorize_salary(self, salary):
        salary = float(salary)
        if salary >= 100000.0:
            return "High"
        elif 50000.0 <= salary < 100000.0:
            return "Medium"
        else:
            return "Low"

    def mapper(self, _, line):
        (name, jobTitle, agencyID, agency, hireDate, annualSalary, grossPay) = line.split('\t')
        salary_category = self.categorize_salary(annualSalary)
        yield salary_category, 1

    def combiner(self, salary_category, counts):
        yield salary_category, sum(counts)

    def reducer(self, salary_category, counts):
        yield salary_category, sum(counts)

if __name__ == '__main__':
    MRSalaries.run()
