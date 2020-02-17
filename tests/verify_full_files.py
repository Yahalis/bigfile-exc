from datetime import datetime as dt
from bigfile_exc import BigFileManager

if __name__ == '__main__':
    def main():
        chunksize = 800000
        names = ['James', 'John', 'Robert', 'Michael', 'William', 'David', 'Richard', 'Charles', 'Joseph', 'Thomas',
                 'Christopher', 'Daniel', 'Paul', 'Mark', 'Donald', 'George', 'Kenneth', 'Steven', 'Edward', 'Brian',
                 'Ronald', 'Anthony', 'Kevin', 'Jason', 'Matthew', 'Gary', 'Timothy', 'Jose', 'Larry', 'Jeffrey',
                 'Frank', 'Scott', 'Eric', 'Stephen', 'Andrew', 'Raymond', 'Gregory', 'Joshua', 'Jerry', 'Dennis',
                 'Walter', 'Patrick', 'Peter', 'Harold', 'Douglas', 'Henry', 'Carl', 'Arthur', 'Ryan', 'Roger']

        for file in ['small1.txt', 'small2.txt', 'big.txt', 'big10.txt', ]:
            filename='files/'+file
            bfm = BigFileManager(names, filename, chunksize)
            start = dt.now()
            bfm.run()
            print(dt.now() - start)
            # verify agains the whole the file
            f = open(filename, 'rt')
            chunk = f.read()
            bfm.aggregator.verify(chunk)

    main()
    print('Done')
