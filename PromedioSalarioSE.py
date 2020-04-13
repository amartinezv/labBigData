from mrjob.job import MRJob

class PromedioSalarioSE(MRJob):

    def mapper(self, _, line):
        idemp, sececon, salary, year = line.split(',')
        try:
            salary = float(salary)
        except ValueError:
            pass
        else:
            yield sececon, salary


    def reducer (self, sececon, salario):
        sumSalario=0
        cont =0
        for s in salario:
            sumSalario += s
            cont += 1

        yield sececon, sumSalario/cont

if __name__ == '__main__':
    PromedioSalarioSE.run()