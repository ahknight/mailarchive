#!/usr/bin/env python3

import os                   # path
import sys                  # stdout, argv, exit
import time                 # sleep
import logging
import argparse

from maildir_lite import Maildir, InvalidMaildirError

from .archive import MailArchive, MailArchiveRecord
from .progress import Progress
from .outputs import QuietOutput, StandardOutput, VerboseOutput, ADDED, UPDATED, EXISTING


def clean_path(path):
    path = os.path.expanduser(path) # Expand the ~ token.
    path = os.path.normpath(path)   # Remove redundant parts: ./././/.
    path = os.path.realpath(path)   # Resolve symlinks and return cannonical path
    return path

def main(argc, argv):
    global STOP, archive, DRY_RUN
    STOP = False
    
    # logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO, stream=sys.stdout)
    logging.basicConfig(format="%(message)s", level=logging.WARNING, stream=sys.stdout)
    PROGRAM = os.path.basename(argv[0])
    
    # Defaults
    Output = StandardOutput
    ARCHIVE_FOLDER = "/Archive"
    CHECK_ARCHIVE = False
    DRY_RUN = False
    RECURSIVE = False
    USE_FS_LAYOUT = False
    
    USER_MAILDIR = None
    if os.getenv("MAILDIR"):
        path = clean_path(os.getenv("MAILDIR"))
        if os.path.exists(path):
            USER_MAILDIR = path
    
    if not USER_MAILDIR:
        USER_MAILDIR = clean_path("~/Maildir")
    
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="archive maildirs",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    parser.add_argument("-q", "--quiet",
                            action="store_true", help="no output")
    parser.add_argument("-v", "--verbose", default=0,
                            action="count", help="show per-message progress and status")
    parser.add_argument("-d", "--debug",
                            action="store_true", help="show everything. everything.")
    parser.add_argument("-m", "--maildir", default=USER_MAILDIR,
                            help="path to maildir to import messages into (will create if nonexistant)")
    parser.add_argument("-a", "--archive", default=ARCHIVE_FOLDER,
                            help="folder in maildir to use as an archive (will create if nonexistant)")
    parser.add_argument("-n", "--dry-run",
                            action="store_true", help="simulate actions only")
    parser.add_argument("-r", "--recursive",
                            action="store_true", help="also import all subfolders")
    parser.add_argument("-l", "--fs",
                            action="store_true", help="use FS layout for archive subfolders instead of Maildir++")
    parser.add_argument("-f", "--fsck",
                            action="store_true", help="verify and repair the archive's index")
    parser.add_argument("maildirs", nargs="+")

    args = parser.parse_args()
    logging.info(args)
    
    ARCHIVE_FOLDER = args.archive
    USER_MAILDIR = args.maildir
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("Debug output enabled.")
    if args.quiet:
        Output = QuietOutput
        logging.debug("Output %r", Output)
        if logging.getLogger().getEffectiveLevel() != logging.DEBUG:
            logging.getLogger().setLevel(logging.WARNING)
    if args.verbose:
        Output = VerboseOutput
        logging.debug("Output %r", Output)
        if logging.getLogger().getEffectiveLevel() != logging.DEBUG:
            logging.getLogger().setLevel(logging.INFO)
    CHECK_ARCHIVE = args.fsck
    DRY_RUN = args.dry_run
    RECURSIVE = args.recursive
    USE_FS_LAYOUT = args.fs
    
    logging.debug("Archive maildir: %s", USER_MAILDIR)
    logging.debug("Archive folder: %s", ARCHIVE_FOLDER)

    # Verify the given paths are Maildirs
    maildir_paths = []
    for path in args.maildirs:
        logging.debug("* Checking path %r", path)
        
        path = clean_path(path)
        
        logging.debug("* Path expanded to %r", path)
        
        try:
            maildir = Maildir(path, fs_layout=USE_FS_LAYOUT)
            maildir_paths.append(path)
            logging.info("+ added folder %s" % (maildir.name,))
            
            if RECURSIVE:
                logging.debug("_ scanning children")
                for folder in maildir.list_folders():
                    try:
                        child = maildir.get_folder(folder)
                        maildir_paths.append(child.path)
                        logging.info("+   added subfolder of %s: %s" % (maildir.name, child.name,))
                    except InvalidMaildirError as e:
                        logging.info(e)
                    
        except InvalidMaildirError as e:
            print(e)
            logging.warning("%s: %s" % (PROGRAM, e.args))
            continue;
            
    # Create the archive maildir and get the direct path to it
    maildir = Maildir(USER_MAILDIR, create=True, fs_layout=USE_FS_LAYOUT)
    ARCHIVE_PATH = maildir.create_folder(ARCHIVE_FOLDER).path
    # print(maildir.path)
    logging.debug("Archive folder: %s", ARCHIVE_FOLDER)
    logging.debug("Archive path: %s", ARCHIVE_PATH)
    del maildir
    
    # Verify the DB before starting
    archive = MailArchive(ARCHIVE_PATH, create=True, lazy=True, fs_layout=USE_FS_LAYOUT)
    archive.maildir.lazy_period = 10
    if CHECK_ARCHIVE:
        archive.check(True)
    
    # Import Maildirs
    if not len(maildir_paths):
        logging.debug("- No maildirs given. Exiting.")
        return 0
        
    # Iterate over maildirs
    for path in sorted(maildir_paths):
        if STOP: break
        
        logging.debug("* Opening %r", path)
        
        # Open the maildir
        source = Maildir(path, lazy=True, xattr=True, fs_layout=USE_FS_LAYOUT)
        source.lazy_period = 10
        
        # Gather list of messages to check.
        msgids = sorted(source.keys())
        msgcount = len(msgids)
        
        logging.debug("* Found %r keys.", msgcount)
        
        with Output(name=source.name, total=msgcount) as output:
            for msgid in msgids:
                result = EXISTING
                
                try:
                    msg = source[msgid]
                except KeyError:
                    logging.error("%s: message not found" % (msgid,))
                    output.increment(result)
                    continue
                
                try:
                    record = archive[msg]
                    if record.should_update(msg):
                        if DRY_RUN:
                            result = UPDATED
                        else:
                            result = archive.update_message(msg)
                except KeyError:
                    if DRY_RUN:
                        result = ADDED
                    else:
                        result = archive.add_message(msg)
                
                output.increment(result)
                if STOP: break
        
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
