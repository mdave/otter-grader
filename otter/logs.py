####################################
##### Logging for Otter-Grader #####
####################################

import re
import os
import pickle
import shelve
import datetime as dt

from enum import Enum, auto
from glob import glob


_SHELF_FILENAME = ".OTTER_ENV"


class QuestionNotInLogException(Exception):
    """Exception that indicates that a specific question was not found in any entry in the log"""


class EventType(Enum):
    """Event types for log entries"""

    AUTH = auto()
    BEGIN_CHECK_ALL = auto()
    BEGIN_EXPORT = auto()
    CHECK = auto()
    END_CHECK_ALL = auto()
    END_EXPORT = auto()
    INIT = auto()
    SUBMIT = auto()
    TO_PDF = auto()


class ShelvedQuestion:
    def __init__(self, question, shelf_files, unshelved):
        self.question = question
        self.shelf_files = shelf_files
        self.unshelved = unshelved

    def __iter__(self):
        return self.shelf_files.keys()

    def __getitem__(self, key):
        return self.shelf_files[key]


class LogEntry:
    """An entry in Otter's log. Tracks event type, grading results, success of operation, and errors
    thrown.

    Args:
        event_type (``otter.logs.EventType``): the type of event for this entry
        results (``list`` of ``otter.ok_parser.OKTestsResult``, optional): the results of grading if 
            this is an ``otter.logs.EventType.CHECK`` record
        question (``str``, optional): the question name for an EventType.CHECK record
        success (``bool``, optional): whether the operation was successful
        error (``Exception``, optional): an error thrown by the process being logged if any
    """

    def __init__(self, event_type, shelf=None, unshelved=[], results=[], question=None, success=True, error=None):
        assert event_type in EventType, "Invalid event type"
        self.event_type = event_type
        self.shelf = shelf
        self.unshelved = []
        self.results = results
        self.question = question
        self.timestamp = dt.datetime.utcnow()
        self.success = success
        self.error = error

    def __repr__(self):
        if self.question:
            return "otter.logs.LogEntry(event_type={}, question={}, success={}, timestamp={})".format(
                self.event_type, self.question, self.success, self.timestamp.isoformat()
            )

        return "otter.logs.LogEntry(event_type={}, success={}, timestamp={})".format(
            self.event_type, self.success, self.timestamp.isoformat()
        )

    def get_results(self):
        """Get the results stored in this log entry
        
        Returns:
            ``list`` of ``otter.ok_parser.OKTestsResult``: the results at this entry if this is an 
                ``otter.logs.EventType.CHECK`` record
        """
        assert self.event_type is EventType.CHECK, "this record type has no results"
        if isinstance(self.results, list):
            return self.results[0]
        return self.results

    def raise_error(self):
        """Raises the error stored in this entry

        Raises:
            ``Exception``: the error stored at this entry, if present
        """
        if self.error is not None:
            raise self.error

    @staticmethod
    def sort_log(log, ascending=True):
        """Sorts a list of log entries by timestamp
        
        Args:
            log (``list`` of ``otter.logs.LogEntry``): the log to sort
            ascending (``bool``, optional): whether the log should be sorted in ascending (chronological) 
                order; default ``True``

        Returns:
            ``list`` of ``otter.logs.LogEntry``: the sorted log
        """
        if ascending:
            return list(sorted(log, key = lambda l: l.timestamp))
        return list(sorted(log, key = lambda l: l.timestamp, reverse = True))


# TODO: update docstring
class Log:
    """A class for reading and interacting with a log

    Args:
        entries (``list`` of ``otter.logs.LogEntry``): the list of entries for this log
        ascending (``bool``, optional): whether the log is sorted in ascending (chronological) order;
            default ``True``
    """

    def __init__(self, entries, ascending=True):
        self.entries = entries
        self.ascending = ascending
        # self.shelves = {}       # maps question names to shelves, stored as ({str: bytes}, list)
        self.shelved_questions = []

    def __repr__(self):
        return "otter.logs.Log([\n  {}\n])".format(",\n  ".join([repr(e) for e in self.entries]))

    def __getitem__(self, idx):
        return self.entries[idx]

    def __iter__(self):
        return iter(self.entries)

    def add_entry(self, event_type, results=[], question=None, success=True, error=None):
        if not self.ascending:
            self.sort()
        entry = LogEntry(event_type, results=results, question=question, success=success, error=error)
        self.entries.append(entry)

    def shelve_question(self, question, env):
        shelf_files, unshelved = Log.shelve_environment(env)
        
        with open(_SHELF_FILENAME + "_" + question, "wb+") as f:
            pickle.dump(ShelvedQuestion(question, shelf_files, unshelved), f)

        self.shelved_questions.append(question)
        
    def unshelve_question(self, question):
        filename = _SHELF_FILENAME + "_" + question
        with open(filename, "rb") as f:
            shelf = pickle.load(f)

        for ext in shelf:
            with open(_SHELF_FILENAME + ext, "wb+") as f:
                f.write(shelf[ext])
        
        shelf = shelve.open(_SHELF_FILENAME)
        return shelf

    # TODO: update docstring
    def to_file(self, filename):
        """Appends this log entry (pickled) to a file 
        
        Args:
            filename (``str``): the path to the file to append this entry
        """
        with open(filename, "wb+") as f:
            pickle.dump(self, f)

    def sort(self, ascending=True):
        self.entries = LogEntry.sort_log(self.entries, ascending=ascending)
        self.ascending = ascending

    def get_questions(self):
        all_questions = [entry.question for entry in self.entries if entry.event_type == EventType.CHECK]
        return list(sorted(set(all_questions)))

    @classmethod
    def from_file(cls, filename):
        """Loads a log from a file

        Args:
            filename (``str``): the path to the log
            ascending (``bool``, optional): whether the log should be sorted in ascending (chronological) 
                order; default ``True``

        Returns:
            ``otter.logs.Log``: the ``Log`` instance created from the file
        """
        try:
            with open(filename, "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            return cls([])

    def get_question_entry(self, question):
        if self.ascending:
            self.entries = LogEntry.sort_log(self.entries)
            self.ascending = False
        for entry in self.entries:
            if entry.question == question:
                return entry
        raise QuestionNotInLogException()

    def get_results(self, question):
        """Gets the most recent grading result for a specified question from this log

        Args:
            question (``str``): the question name to look up

        Returns:
            ``otter.ok_parser.OKTestsResult``: the most recent result for the question

        Raises:
            ``otter.logs.QuestionNotInLogException``: if the question is not found
        """
        return self.get_question_entry(question).get_results()

    @staticmethod
    def shelve_environment(env):
        if glob(_SHELF_FILENAME + ".*"):
            os.system(f"rm -f {_SHELF_FILENAME}.*")
        unshelved = []
        with shelve.open(_SHELF_FILENAME) as shelf:
            for k, v in env.items():
                try:
                    shelf[k] = v
                except:
                    unshelved.append(k)
        
        shelf_files = {}
        for file in glob(_SHELF_FILENAME + "*"):
            ext = re.sub(_SHELF_FILENAME, "", file)
            f = open(file, "rb")
            shelf_files[ext] = f.read()
            f.close()
            
        return shelf_files, unshelved
