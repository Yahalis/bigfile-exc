from collections import defaultdict

class WordMatch:
    """
    Data class to hold one word match data.
    Line and offset are at the chunk level.
    """
    def __init__(self, line: int, offset: int, chunkId: int):
        self.line = line
        self.offset = offset
        self.chunkId = chunkId

class ChunkMatches:
    """
    This class holds the matches data in the chunk level and can aggregate multiple chunks to file level.
    note that lineCount info is not relevant for the file level and is manages in the Aggregator class.
    """
    def __init__(self, charOffset: int, lineCount: int = 0):
        self.wordMatches = defaultdict(list)
        self.charOffset = charOffset
        self.lineCount = lineCount

    def add(self, word: str, line: int, offset: int, chunkId:int) -> None:
        "insert a new word-match info"
        self.wordMatches[word].append(WordMatch(line, offset, chunkId))

    def append(self, chunkMatches)->None:
        """
        Append the matching data from the chunk.
        :param chunkMatches:
        :return:
        """
        for word, matchesList in chunkMatches.wordMatches.items():
            for wm in matchesList:
                self.add(word, wm.line, wm.offset, wm.chunkId)

    @property
    def lineCount(self) -> int:
        return self.__lineCount

    @lineCount.setter
    def lineCount(self, x: int) -> None:
        self.__lineCount = x

    def __str__(self):
        ret = ''
        for word, matchList in self.wordMatches.items():
            ret += word + ' -> ['
            for wm in matchList:
                ret += '[lineOffset={}, charOffset={}],'.format(wm.line, wm.offset)
            ret = ret[:len(ret) - 1] + ']\n'
        return ret

