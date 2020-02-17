import queue
import threading
import time

import nltk

from bigfile_exc import ChunkMatches
from bigfile_exc import Aggregator

class Matcher:
    def __init__(self, aggregator: Aggregator, maxsize=50) -> None:
        """
        Initialize the queues and keep params
        :param aggregator: Aggregator instance to use
        :param maxsize: Maximul queue size, will force chunker to wait if full
        """
        self.__aggregator = aggregator
        self.__matchQueue = queue.Queue(maxsize=maxsize)
        self.__sentinel = 'Done'
        self.__counterLock=threading.Lock()

    def run(self, numWorkers:int) -> None:
        """
        Running the workers.
        :param numWorkers: num workers to run.
        :return:
        """
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
        """
        worker is listening on the queue and doing the match of the given chunk chunk.
        :param id: worker id (FFU)
        :return:
        """
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
        """
        Put termination marker at the end of the Queue (per worker)
        :return:
        """
        for i in range(self.__numWorkers):
            self.__matchQueue.put(self.__sentinel, block=True)

    def match(self, chunk: str, names: set, offset: int, chunkId: int) -> None:
        "Put match instructions in the queue"
        # this will wait if the Queue is full
        self.__matchQueue.put({'chunk': chunk, 'names': names, 'offset': offset, 'chunkId': chunkId}, block=True)

    def do_match(self, chunk: str, names: set, offset: int, chunkId: int) -> None:
        """
        Main matching function.
        using nltk regexp_span_tokenize to get all word locations, based on the letters not constructing tokens as splitters.
        keeping the newline legal in order to be able to easily find and count lines, thus special treatmet if newline is in the token.
        :param chunk: the chunk to match
        :param names: set of names
        :param offset: offset of the chunk in the file.
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

