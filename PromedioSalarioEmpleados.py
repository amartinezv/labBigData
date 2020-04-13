from mrjob.job import MRJob

class PromedioSalarioEmpleados(MRJob):

    def mapper(self, _, line):
        idemp, sececon, salary, year = line.split(',')
        try:
            salary = float(salary)
        except ValueError:
            pass
        else:
            yield idemp, salary


    def reducer (self, idemp, salario):
        sumSalario=0
        cont =0
        for s in salario:
            sumSalario += s
            cont += 1

        yield idemp, sumSalario/cont

if __name__ == '__main__':
    PromedioSalarioEmpleados.run()