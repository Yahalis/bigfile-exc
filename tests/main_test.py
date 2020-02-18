from datetime import datetime as dt
from bigfile_exc import BigFileManager

if __name__ == '__main__':
    def main():
        chunksize = 800000
        filename='files/big.txt'
        names = ['James', 'John', 'Robert', 'Michael', 'William', 'David', 'Richard', 'Charles', 'Joseph', 'Thomas',
                 'Christopher', 'Daniel', 'Paul', 'Mark', 'Donald', 'George', 'Kenneth', 'Steven', 'Edward', 'Brian',
                 'Ronald', 'Anthony', 'Kevin', 'Jason', 'Matthew', 'Gary', 'Timothy', 'Jose', 'Larry', 'Jeffrey',
                 'Frank', 'Scott', 'Eric', 'Stephen', 'Andrew', 'Raymond', 'Gregory', 'Joshua', 'Jerry', 'Dennis',
                 'Walter', 'Patrick', 'Peter', 'Harold', 'Douglas', 'Henry', 'Carl', 'Arthur', 'Ryan', 'Roger']
        bfm = BigFileManager(names, filename, chunksize, numWorkers=8)
        start = dt.now()
        bfm.run()
        bfm.aggregator.print()
        print('Time:', dt.now() - start)

    main()
    print('Done')
