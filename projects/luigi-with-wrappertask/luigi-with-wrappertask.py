import luigi 
from time import sleep
import random


class RandomNumbers(luigi.Task):
    task_namespace = "test"
    random_param = luigi.Parameter(default= str(random.random()) )
    def output(self):
        return luigi.LocalTarget('data/random.txt')

    def run(self):
        numbers = [i for i in range(100)]

        sleep(3)
        with self.output().open('w') as f:
            for number in numbers:
                f.write('{}\n'.format(number))

class NoOutPut(luigi.Task):
    task_namespace = "test"

    random_param = luigi.Parameter(default= str(random.random()) )

    def run(self):
        sleep(3)
        with open('data/nothing.txt','w') as f:
            f.write('nothing')
        print('No output is done')

    def complete(self):
        if random.random() > 0.5:
            return True
        else:
            return False


class AllTasks(luigi.WrapperTask):
    task_namespace = "test"
    random_param = luigi.Parameter(default= str(random.random()) )

    def requires(self):
        return RandomNumbers(), NoOutPut()


if __name__ == "__main__":
    # pass
    luigi.run(['test.AllTasks', '--workers', '1', '--local-scheduler'] )