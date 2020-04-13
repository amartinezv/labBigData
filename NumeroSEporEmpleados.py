from mrjob.job import MRJob

class NumeroSEporEmpleados(MRJob):

    def mapper(self, _, line):
        idemp, sececon, salary, year = line.split(',')
        try:
            salary = float(salary)
        except ValueError:
            pass
        else:
            yield idemp, sececon


    def reducer (self, idemp, sector):
        cont =0
        for s in sector:
            cont += 1

        yield idemp, cont

if __name__ == '__main__':
    NumeroSEporEmpleados.run()