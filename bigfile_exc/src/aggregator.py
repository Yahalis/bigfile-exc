import queue
import threading

from bigfile_exc import ChunkMatches


class Aggregator:
    """
    This class is responsible for the aggregation task.
    It has a queue where it gets the partial chunk results (matches) and aggregates them
    it is also responsible for printing the final output.
    """

    def __init__(self):
        """
        Initializing the counters
        """
        self.__lineCounters = dict()
        self.__lineOffsets = dict()
        self.__chunkMatches = ChunkMatches(0)
        self.__aggQueue = queue.Queue()
        self.__terminateQueue = queue.Queue()
        self.__sentinel = 'Done'

    def run(self) -> None:
        """ Start the aggregation worker. """
        self.__worker = threading.Thread(
            name='aggregate_worker',
            target=self.worker,
        )
        self.__worker.start()

    def worker(self) -> None:
        """
        get chunk-matches from the quueue and call aggregate.
        When done - signal the ternminate queue
        """
        nChunks = 0
        for kwargs in iter(self.__aggQueue.get, self.__sentinel):
            self.aggregate(**kwargs)
            nChunks += 1

        # Calculating the line offset per chunk from the line counters.
        nLines = 0
        for i in range(1, nChunks + 1):
            self.__lineOffsets[i] = nLines
            nLines += self.__lineCounters[i] - 1

        self.__terminateQueue.put(self.__sentinel)

    def terminate(self) -> None:
        """
        puts the sentinel in the agg requests queue to terminate the worker.
        :return:
        """
        self.__aggQueue.put(self.__sentinel)

    def await_terminate(self):
        self.__terminateQueue.get()

    def put_agg_data(self, chunkId: int, chunkMatches: ChunkMatches):
        self.__aggQueue.put({'chunkMatches': chunkMatches, 'chunkId': chunkId})

    def aggregate(self, chunkId: int, chunkMatches: ChunkMatches) -> None:
        """
        Aggregate adds the ChunkMatches of a certain chunk into the main ChunckMatches
        :param chunkId:
        :param chunkMatches:
        :return:
        """
        assert chunkId not in self.__lineCounters, "can't aggregate the same chunk twice."
        matches = self.__chunkMatches
        self.__lineCounters[chunkId] = chunkMatches.lineCount
        matches.append(chunkMatches)

    def __str__(self):
        """
        str will also support the printing and takes the number of lines per chunk into account.
        :return:
        """
        if len(self.__lineCounters) != len(self.__lineOffsets):
            return "Aggregation is still in progress"
        # generating the string taking the line-offsets into account.
        ret = ''
        for word, matchList in self.__chunkMatches.wordMatches.items():
            ret += word + ' -> ['
            for wm in matchList:
                ret += '[lineOffset={}, charOffset={}],'.format(wm.line + self.__lineOffsets[wm.chunkId], wm.offset)
            ret = ret[:len(ret) - 1] + ']\n'
        return ret

    def print(self):
        print(self)

    ####################################################
    # test function
    def verify(self, chunk: str):
        """
        This function enables to load in the chunk of the file processed earlier and verify that all words are
        indeed in the correct place
        :param chunk: a full file chunk
        :return:
        """
        cm = self.__chunkMatches
        count = 0
        errcount = 0
        for word, matchList in cm.wordMatches.items():
            wl = len(word)
            for wm in matchList:
                count += 1
                if chunk[wm.offset:wm.offset + wl].capitalize() != word:
                    print(chunk[wm.offset:wm.offset + wl], word, wm.offset)
                    errcount += 1
        print('Verification: num words: {}, num errors: {}'.format(count, errcount))
        return
