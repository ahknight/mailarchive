import os                   # path
import sys                  # stdout, argv, exit
import time                 # sleep
import logging

from getopt import getopt, GetoptError      # getopt, GetoptError

# PIXIE SPARKLE MAGIC DUST
import multiprocessing as multiprocessing

from maildir_lite import Maildir, InvalidMaildirError

from .mailarchive import MailArchive, MailArchiveRecord
from .progress import Progress
from .outputs import QuietOutput, StandardOutput, VerboseOutput, ADDED, UPDATED, EXISTING

def worker_init(*args):
    '''
    Creates some process-specific globals for speed and to avoid concurrency issues.
    '''
    global archive, dry_run
    archive_path, dry_run = args

    archive = MailArchive(archive_path, lazy=True)
    # archive.maildir.lazy_period = 10

def process_message(args):
    '''
    Process a single message.  Expects to be run on a Process spawned from a Pool where worker_init has been run.
    Returns EXISTING, ADDED, or UPDATED as appropriate to the action taken on the message.
    '''
    global archive, dry_run
    
    result = EXISTING
    
    maildir, msgid = args
    msg = maildir[msgid]
    
    try:
        record = archive[msg]
        if record.should_update(msg):
            if dry_run:
                result = UPDATED
            else:
                result = archive.update_message(msg)
    except KeyError:
        if dry_run:
            result = ADDED
        else:
            result = archive.add_message(msg)
    
    return (msg.msgid, result)

def clean_path(path):
    path = os.path.expanduser(path)
    path = os.path.normpath(path)
    path = os.path.realpath(path)
    return path

def main(argc, argv):
    global STOP
    STOP = False
    
    # logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO, stream=sys.stdout)
    logging.basicConfig(format="%(message)s", level=logging.WARNING, stream=sys.stdout)
    PROGRAM = os.path.basename(argv[0])
    
    # Defaults
    Output = StandardOutput
    CHUNK_SIZE = 50
    ARCHIVE_FOLDER = "/Archive"
    MULTIPROCESSING = True
    CHECK_ARCHIVE = False
    DRY_RUN = False
    RECURSIVE = False
    
    USER_MAILDIR = None
    if os.getenv("MAILDIR"):
        path = clean_path(os.getenv("MAILDIR"))
        if os.path.exists(path):
            USER_MAILDIR = path
    
    if not USER_MAILDIR:
        USER_MAILDIR = clean_path("~/Maildir")
    
    # Parse arguments
    help_text = "usage: %s [opts] maildir ...\n" % PROGRAM
    short_args = ""
    long_args = []
    args = (
    #   short   long            args    help
        ("h",   "help",         False,  "Show help.\n"),
        
        ("q",   "quiet",        False,  "No output."),
        ("v",   "verbose",      False,  "Show per-message progress and status."),
        ("d",   "debug",        False,  "Show everything. Everything.\n"),
        
        ("m",   "maildir",      True,   "Path to maildir to import messages into (will create if nonexistant; default: %s)." % USER_MAILDIR),
        ("a",   "archive",      True,   "Folder in maildir to use as an archive (will create if nonexistant; default: %s)." % ARCHIVE_FOLDER),
        
        ("n",   "dry-run",      False,  "Simulate import."),
        ("1",   "one",          False,  "Disable multiprocessing."),
        ("r",   "recursive",    False,  "Also import subfolders.\n"),
        
        ("f",   "fsck",         False,  "Verify the archive and repair any issues.\n"),
        
        ("c",   "chunk-size",   True,   "Minimum size of a work unit (advanced; default: %d)." % CHUNK_SIZE),
    )
    
    # Build long and short argument ... arguments.
    for arg in args:
        if len(arg[3]):
            help_text += "  -%s, --%-12s %s\n" % ( arg[0], arg[1], arg[3] )
            
        if arg[2]:
            short_args += arg[0]
            short_args += ":"
            long_args.append(arg[1] + "=")
        else:
            short_args += arg[0]
            long_args.append(arg[1])
            
    try:
        (options, paths) = getopt(argv[1:], short_args, long_args)
    except GetoptError as e:
        logging.error("%s: %s" % (PROGRAM, e))
        return 1
        
    for option, value in options:
        logging.debug("OPTION: %r %r", option, value)
        if option in ["-a", "--archive"]:
            ARCHIVE_FOLDER = value
            logging.debug("ARCHIVE_FOLDER %r", ARCHIVE_FOLDER)
            
        elif option in ["-m", "--maildir"]:
            USER_MAILDIR = clean_path(value)
            logging.debug("USER_MAILDIR %r", USER_MAILDIR)
            
        elif option in ["-q", "--quiet"]:
            Output = QuietOutput
            logging.debug("Output %r", Output)
            if logging.getLogger().getEffectiveLevel() != logging.DEBUG:
                logging.getLogger().setLevel(logging.WARNING)
            
        elif option in ["-d", "--debug"]:
            logging.getLogger().setLevel(logging.DEBUG)
            logging.debug("Debug output enabled.")
            
        elif option in ["-v", "--verbose"]:
            Output = VerboseOutput
            logging.debug("Output %r", Output)
            if logging.getLogger().getEffectiveLevel() != logging.DEBUG:
                logging.getLogger().setLevel(logging.INFO)
            
        elif option in ["-f", "--fsck"]:
            CHECK_ARCHIVE = True
            logging.debug("CHECK_ARCHIVE %r", CHECK_ARCHIVE)
            
        elif option in ["-n", "--dry-run"]:
            DRY_RUN = True
            logging.debug("DRY_RUN %r", DRY_RUN)
            
        elif option in ["-r", "--recursive"]:
            RECURSIVE = True
            logging.debug("RECURSIVE %r", RECURSIVE)
            
        elif option in ["-1", "--one"]:
            MULTIPROCESSING = False
            logging.debug("MULTIPROCESSING %r", MULTIPROCESSING)
            
        elif option in ["-c", "--chunk-size"]:
            CHUNK_SIZE = int(value)
            if CHUNK_SIZE < 1: CHUNK_SIZE = 1
            logging.debug("CHUNK_SIZE %r", CHUNK_SIZE)
            
        elif option in ["-h", "--help"]:
            logging.debug("HELP")
            print(help_text)
            sys.exit()
            
    # Verify the given paths are Maildirs
    maildir_paths = []
    for path in paths:
        logging.debug("* Checking path %r", path)
        
        path = clean_path(path)
        
        logging.debug("* Path expanded to %r", path)
        
        try:
            maildir = Maildir(path)
            maildir_paths.append(path)
            logging.info("+ added folder %s" % (maildir.name,))
            
            if RECURSIVE:
                for folder in maildir.list_folders():
                    child = maildir.get_folder(folder)
                    maildir_paths.append(child.path)
                    logging.info("+   added subfolder of %s: %s" % (maildir.name, child.name,))
                    
        except InvalidMaildirError:
            logging.warning("%s: %s: not a maildir; skipped." % (PROGRAM, path,))
            continue;
            
    # Create the archive maildir and get the direct path to it
    maildir = Maildir(USER_MAILDIR, create=True)
    ARCHIVE_PATH = maildir.create_folder(ARCHIVE_FOLDER).path
    # print(maildir.path)
    # print(ARCHIVE_FOLDER)
    # print(ARCHIVE_PATH)
    del maildir
    
    # Verify the DB before starting
    archive = MailArchive(ARCHIVE_PATH, create=True)
    if CHECK_ARCHIVE:
        archive.check(True)
    del archive
    
    # Import Maildirs
    if not len(maildir_paths):
        logging.debug("- No maildirs given. Exiting.")
        return 0
        
    # Iterate over maildirs
    for path in sorted(maildir_paths):
        if STOP: break
        
        logging.debug("* Opening %r", path)
        
        # Open the maildir
        source = Maildir(path, lazy=True)
        
        # Gather list of messages to check.
        msgids = sorted(source.keys())
        msgcount = len(msgids)
        
        logging.debug("* Found %r keys.", msgcount)
        
        with Output(name=source.name, total=msgcount) as output:
            
            imap_args = [(source, msgid) for msgid in msgids]
            CHUNK_SIZE = max(CHUNK_SIZE, int(msgcount/256))
            
            if MULTIPROCESSING:
                with multiprocessing.Pool(initializer=worker_init, initargs=(ARCHIVE_PATH, DRY_RUN)) as pool:
                    imap_results = pool.imap(process_message, imap_args, chunksize=CHUNK_SIZE)
                    
                    for result in imap_results:
                        msgid, mark = result
                        output.increment(mark)
                        if STOP: break
                    
                    if not STOP:
                        pool.close()
                    else:
                        pool.terminate()
                        
                    pool.join()
                    
                    del imap_args, imap_results
            else:
                worker_init(ARCHIVE_PATH, DRY_RUN)
                for args in imap_args:
                    msgid, mark = process_message(args)
                    output.increment(mark)
                    if STOP: break
                del imap_args
                
        del source, msgids
    
    if STOP: return 1
        
def start():
    global STOP
    
    import signal
    def signal_handler(sig, frame):
        global STOP
        if STOP:
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            os.kill(os.getpid(), signal.SIGTERM)
        STOP = True
    signal.signal(signal.SIGINT, signal_handler)
    
    # You might be a C developer if...
    
    STOP = False
    argc = len(sys.argv)
    argv = sys.argv
    
    sys.exit(main(argc, argv))
    
if __name__ == "__main__":
    start()
