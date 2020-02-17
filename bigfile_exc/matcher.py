import queue
import threading
import time

import nltk

from bigfile_exc import ChunkMatches
from bigfile_exc import Aggregator


class Matcher:
    def __init__(self, aggregator: Aggregator, maxsize=50):
        self.__aggregator = aggregator
        self.__matchQueue = queue.Queue(maxsize=maxsize)
        self.__sentinel = 'Done'
        self.__counterLock=threading.Lock()

    def run(self, numWorkers=4) -> None:
        self.__numWorkers = numWorkers
        self.__doneWorkers=0
        self.__workers = [threading.Thread(
            name='matching_worker {}'.format(i),
            target=self.worker,
            args=(i,),
        ) for i in range(numWorkers)]
        for c in self.__workers:
            c.start()


    def worker(self, id: int) -> None:
        # print('start matching worker #'+str(id+1))
        for aggData in iter(self.__matchQueue.get, self.__sentinel):
            kwargs = aggData
            self.do_match(**kwargs)

        # marking myself as finished
        with self.__counterLock:
            self.__doneWorkers+=1

        # terminating aggregator if I'm last.
        if self.__doneWorkers==self.__numWorkers:
            self.__aggregator.terminate()

    def terminate(self) -> None:
        for i in range(self.__numWorkers):
            self.__matchQueue.put(self.__sentinel, block=True)

    def match(self, chunk: str, names: set, offset: int, chunkId: int) -> None:
        self.__matchQueue.put({'chunk': chunk, 'names': names, 'offset': offset, 'chunkId': chunkId}, block=True)

    def do_match(self, chunk: str, names: set, offset: int, chunkId: int) -> None:
        """

        :param chunk:
        :param names:
        :param offset:
        :return:
        """
        nLines = 1
        chunkMatches = ChunkMatches(offset)
        tokens = nltk.regexp_span_tokenize(chunk, regexp='[^A-Za-z0-9_\n]')
        for s, e in tokens:
            word = chunk[s:e].lower()
            newLinePos = word.find('\n')
            if newLinePos < 0:
                if word in names:
                    chunkMatches.add(word.capitalize(), nLines, offset + s, chunkId)
            else:  # handling newlines in the pattern
                words = word.split('\n')
                for subword in words:
                    if subword in names:
                        chunkMatches.add(subword.capitalize(), nLines, offset + s, chunkId)
                    nLines += 1
                    s += len(subword) + 1
                # split has +1 items than newlines, so we need to count back one line.
                nLines -= 1
        chunkMatches.lineCount = nLines
        self.__aggregator.put_agg_data(chunkId, chunkMatches)


if __name__ == '__main__':
    def test():
        pass
