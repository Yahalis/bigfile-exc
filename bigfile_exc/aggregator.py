import queue
import threading
from collections import Counter

import nltk

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
        # make sure that we terminate the worker thread gracefully even if an error occures.
        try:
            nChunks = 0
            for kwargs in iter(self.__aggQueue.get, self.__sentinel):
                self.aggregate(**kwargs)
                nChunks += 1

            # Calculating the line offset per chunk from the line counters.
            nLines = 0
            for i in range(1, nChunks + 1):
                if i in self.__lineCounters:
                    self.__lineOffsets[i] = nLines
                    nLines += self.__lineCounters[i] - 1
                else:
                    print("Error in aggregator, missing data on chunk {}".format(i))
                    # break
        except Exception as e:
            print("An error occured in aggregator".format(id))
            raise e
        finally:
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
            return "Aggregation is still in progress or an error occured and some chunks are missing"
        # generating the string taking the line-offsets into account.
        ret = ''
        # print('Num words: {}, {}'.format(len(self.__chunkMatches.wordMatches), self.__chunkMatches.wordMatches.keys()))
        for word, matchList in self.__chunkMatches.wordMatches.items():
            ret += word + ' -> ['
            for wm in matchList:
                ret += '[lineOffset={}, charOffset={}],'.format(wm.line + self.__lineOffsets[wm.chunkId], wm.offset)
            ret = ret[:len(ret) - 1] + ']\n'
        return ret

    def print(self):
        print(self)

    ####################################################
    # test functions
    def verify_offsets(self, chunk: str):
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

    def verify_counts(self, chunk: str, names:set):
        """

        :param chunk:
        :param names:
        :return:
        """
        cm = self.__chunkMatches
        count = 0
        errcount = 0
        wordlist=nltk.word_tokenize(chunk)
        wordsdict=Counter()
        for word in wordlist:
            wordsdict[word]+=1
        for name in names:
            wc = len(cm.wordMatches[name])
            wc1= wordsdict[name]
            if wc!=wc1:
                print('Verificationerror for {} Agg words: {}, nltk words: {}'.format(name, wc, wc1))
        return
