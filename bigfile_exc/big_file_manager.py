import os

from bigfile_exc import Matcher
from bigfile_exc import Aggregator

class BigFileManager:
    """
    This is the main class for the BigID excercise of finding names in a big file.
    """

    def __init__(self, names: list, filename: str, chunksize: int = 80000, numWorkers:int=10) -> None:
        """
        initialize the main class:
        :param names: list of names to find in the file
        :param filename: file-name to check
        :param chunksize: size of the chunks of the file. default is 80000 which approximates 1000 lines.
        """
        # make sure the file exists
        if not os.path.exists(filename):
            raise FileNotFoundError('File {filename} not found'.format(filename=filename))

        self.__names = {name.lower() for name in names}
        self.__filename = filename
        self.__chunksize = chunksize
        self.__numWorkers=numWorkers
        self.aggregator = Aggregator()
        maxQueueSize=1e9//chunksize+1 ## not more than 1GB memory
        self.__matcher = Matcher(self.aggregator, maxsize=maxQueueSize)

    # @property
    def aggregator(self):
        return self.aggregator

    def run(self):
        # Start matcher & aggregator
        self.__matcher.run(numWorkers=self.__numWorkers)
        self.aggregator.run()

        # run the chunker
        self.chunker(self.__filename, self.__chunksize)

        # Send Terminate to the matcher workers & wait for the aggregator to finish
        self.__matcher.terminate()
        self.aggregator.await_terminate()

        # print
        # self.aggregator.print()

    def chunker(self, filename: str, chunksize: int) -> int:
        """
        The chunker read the file in chunks of the defined size.
        as it need to secure full lines, it will also look into the next chunk and get the end of the line (if required)
        :param filename: file-name to check
        :param chunksize: size of the chunks of the file.
        :return: number of chunks.
        """
        f = open(filename, 'r')
        # reading the first line outside the loop so we can always look into the next chunk
        prevChunk = f.read(chunksize)
        counter = 0
        offset = 0
        while True:
            chunk = f.read(chunksize)
            # if the chunk did not end with newline:
            completeNL = False
            l = len(prevChunk)
            if chunk:
                if prevChunk[l - 1] != '\n':
                    i = chunk.find('\n')
                    if i >= 0:  # append the rest of the line
                        prevChunk += chunk[:i + 1]
                        l = len(prevChunk)
                        completeNL = True
                    else:  # or all chunk in case chunk is part of a line (not likely, but worth to check)
                        prevChunk += chunk
                        continue
            # Send the chunk for matching:
            counter += 1
            self.__matcher.match(prevChunk, self.__names, offset, counter)
            offset += l
            # print(counter, l, offset)
            #
            if completeNL:
                prevChunk = chunk[i + 1:]
            else:
                prevChunk = chunk
            if not chunk:
                break  # done
            # break
        return counter
