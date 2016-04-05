import logging
from .progress import *

ADDED = "+"
UPDATED = "^"
EXISTING = "."

log = logging.getLogger(__name__)

class QuietOutput(object):
    '''
    The goggles.
    '''
    def __init__(self, *args, **kwargs):
        pass
    def __enter__(self, *args, **kwargs):
        return self
    def __exit__(self, *args, **kwargs):
        pass
    def increment(self, *args, **kwargs):
        pass

class VerboseOutput(object):
    '''
    Spews a stream of indicators as messages are processed.
    '''
    name = ""
    total = 0
    existing = 0
    added = 0
    updated = 0
    
    def __init__(self, name="", total=0):
        self.name = name
        self.total = total
    
    def __enter__(self):
        log.info("Processing %s (%d messages)" % (self.name, self.total))
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        log.info("\n%s: %d existing; %d new; %d updated\n" % (self.name, self.existing, self.added, self.updated))
        
    def increment(self, mark=""):
        if mark == EXISTING: self.existing += 1
        if mark == ADDED: self.added += 1
        if mark == UPDATED: self.updated += 1
        print(mark, end="", flush=True)

class StandardOutput(object):
    '''
    Outputs a nice scp-like status with progress information and an ETR (estimated time remaining).
    '''
    name = ""
    total = 0
    count = 0
    progress = None
    last_display = 0
    clreol = ""
    
    def __init__(self, name="", total=0):
        self.name = name
        self.total = total
        self.progress = Progress(total)
        
        if len(name) > 40:
            self.name = name[0:19] + "…" + name[-20:]
        
        try:
            import curses
            curses.setupterm()
            self.clreol = curses.tigetstr("el").decode("ascii")
        except:
            import subprocess
            self.clreol = subprocess.getoutput("tput el")
    
    def __enter__(self):
        # print("* %s (%d messages):" % (self.name, self.total) )
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        if self.total != self.count:
            print("", flush=True)
        
    def _format_seconds(self, seconds):
        minute = 60
        hour = 60 * minute
        day = 24 * hour
    
        days = int(seconds / day)
        seconds -= days * day
    
        hours = int(seconds / hour)
        seconds -= hours * hour
    
        minutes = int(seconds / minute)
        seconds -= minutes * minute
    
        secs = seconds
    
        string = ""
        if days > 0: string += "%02d:" % days
        if hours > 0: string += "%02d:" % hours
        if minutes or secs or len(string):
            string += "%02d:%02d" % (minutes, secs)
        else:
            string = "--:--"
            
        return string
    
    def increment(self, mark=""):
        self.count += 1
        self.progress.increment()
        
        if (self.count == 1) or (self.count == self.total) or (time.time() - self.last_display) > 1:
            self.last_display = time.time()
            
            if self.count == self.total:
                # Final stats
                pct = self.progress.percentage()
                secs_remaining = self.progress.time_elapsed()
                mps = self.progress.overall_rate()
                eta = "%5s" % (self._format_seconds(secs_remaining),)
                end = "\n"
                
            else:
                # Current stats
                pct = self.progress.percentage()
                secs_remaining = self.progress.time_remaining()
                mps = self.progress.predicted_rate()
                eta = "%5s ETR" % (self._format_seconds(secs_remaining),)
                end = "\r"
            
            print(self.clreol, end="")
            print("%-40s  %3d%%  %8d  %7.1fm/s  %s" % (self.name, pct, self.count, mps, eta), end=end, flush=True)
