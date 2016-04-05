import logging
import os                      # path
from datetime import datetime  # now()

from .outputs import QuietOutput, StandardOutput, VerboseOutput, ADDED, UPDATED, EXISTING

from maildir_lite import Maildir
#from simplekvs import SQLiteStore as kvs
from simplekvs import SQLAlchemyStore as kvs

log = logging.getLogger(__name__)

# Catch interrupts
import signal
class CancelHandler(object):
    STOP = False
    old_handler = None
    
    def __call__(self, sig, frame):
        self.STOP = True
        
    def __enter__(self):
        self.old_handler = signal.signal(signal.SIGINT, self)
        return self
        
    def __exit__(self, *args):
        signal.signal(signal.SIGINT, self.old_handler)


class MailArchiveRecord(object):
    delimiter = "::"
    def __init__(self, string=None, content_hash=None, msgid=None, flags="", mtime=0, folder=None):
        if string:
            parts = str(string).split(self.delimiter)
            if len(parts) == 4:
                self.folder, self.msgid, self.flags, self.mtime = parts
                self.mtime = float(self.mtime)
            else:
                log.critical("invalid record data: %r -> %r", string, parts)
                raise
        else:
            self.content_hash = content_hash
            self.msgid = msgid
            self.flags = flags
            self.mtime = float(mtime)
            self.folder = folder
    
    def __str__(self):
        return self.delimiter.join( (self.folder, self.msgid, self.flags, repr(self.mtime)) )
    
    def merge_flags(self, newflags):
        self.flags = "".join( sorted( set(self.flags).union(set(newflags)) ) )
    
    def should_update(self, msg):
        return ( (self.mtime > msg.mtime) or (not set(msg.flags).issubset(set(self.flags))) )


class MailArchive(object):
    maildir = None
    store = None
    
    def __init__(self, path, create=True, lazy=False, fs_layout=False):
        self.maildir = Maildir(path, create=create, lazy=lazy, fs_layout=fs_layout)
        self.folders = {folder: self.maildir.get_folder(folder) for folder in self.maildir.list_folders()}
        
        storepath = os.path.join(path, "archive.db")
        self.store = kvs(storepath)
        
    def __getitem__(self, msg):
        key = msg.content_hash
        value = self.store[key]
        return MailArchiveRecord(value)
        
    def __contains__(self, msg):
        try:
            result = self[msg]
            return True
        except KeyError:
            return False
    
    def _folder_for_message(self, msg):
        '''Determine the folder to add the message to.'''
        # Start with the archive folder.
        foldername = self.maildir.name
        
        # Check for major flags.
        if "D" in msg.flags:
            # Draft
            foldername += "/Drafts"
        elif "T" in msg.flags:
            # Trash
            foldername += "/Trash"
        else:
            # Start the general case by suffixing the year of the message.
            d = msg.date
            foldername += "/%04d" % (d.year,)
            
            # Then suffix a folder based on the kind of message (like sent mail or Apple Mail IMAP headers).
            headers = msg.headers
            if headers:
                if headers["X-Uniform-Type-Identifier"] == "com.apple.mail-note":
                    foldername += "/Notes"
                
                elif headers["X-Uniform-Type-Identifier"] == "com.apple.mail-todo":
                    foldername += "/Apple Mail To Do"
                
                elif not (headers['Delivered-To'] or headers['Received']):
                    # Sent or received?
                    foldername += "/Sent"
        
        # Create and cache the folder if it doesn't exist.
        if not foldername in self.folders:
            folder = self.maildir.create_folder(foldername)
            self.folders[foldername] = folder
        
        return self.folders[foldername]
    
    def add_message(self, msg):
        folder = self._folder_for_message(msg)
        
        # Add the message.  We need to add to the folder first to get the final message ID.
        try:
            msgid = folder.add_message(msg)
            record = MailArchiveRecord(content_hash=msg.content_hash, mtime=msg.mtime, msgid=msgid, flags=msg.flags, folder=folder.name)
            self.store[msg.content_hash] = str(record)
            return ADDED
            
        except KeyError:
            # Raised by the KV store if the sum already exists.
            # Clean up and then call update instead.
            if msgid in folder: folder.remove(msgid)
            return self.update_message(msg)
            
    def update_message(self, msg):
            # Fetch the existing record.
            record = self[msg]
            
            # See if there are any properties that need updating.
            if not record.should_update(msg):
                return EXISTING
                
            # Fetch archved message
            archive_msg = self.folders[record.folder][record.msgid]
            
            # Merge flags
            archive_msg.add_flags(msg.flags)
            record.flags = archive_msg.flags
                
            # Earliest date
            if archive_msg.mtime > msg.mtime:
                archive_msg.mtime = msg.mtime
                record.mtime = msg.mtime
                
            # Update mailbox
            self.folders[record.folder].update(record.msgid, archive_msg)
            
            # Update record
            del self.store[archive_msg.content_hash]
            self.store[archive_msg.content_hash] = str(record)
            
            return UPDATED
            
    def check(self, repair=True):
        errors = []
        
        deletes = 0
        updates = 0
        adds = 0
        
        # This takes forever if we don't cache the listing.
        was_lazy = self.maildir.lazy
        self.maildir.lazy = True
        
        # Get the logging level and try to respect it.
        level = log.getEffectiveLevel()
        if level <= logging.INFO:
            Output = VerboseOutput
        elif level <= logging.WARNING:
            Output = StandardOutput
        else:
            Output = QuietOutput
        
        log.warning("* Checking archive %s", self.maildir.name)
        
        with CancelHandler() as handler:
            idx = 0
            count = len(self.store)
            interval = max(1, int(count/100))
            
            log.debug("KVS has %d records.", count)
            
            # Iterate over all the keys in the KV store.
            with self.store as transaction:
                with Output(name="Records (check)", total=count) as output:
                    for key in transaction:
                        # Check to see if ^C has been hit.
                        if handler.STOP: break
                    
                        # Recreate the record object from the store's value.
                        try:
                            record_str = transaction[key]
                            record = MailArchiveRecord(record_str)
                        except KeyError:
                            # "None" is a valid key to the KVS, but not to us.
                            if key is None:
                                continue
                            else:
                                raise
                        
                        # Start the check.
                        delete = False
                        update = False
                        msg = None
                
                        # Check for outright invalid contents.
                        if record.msgid == None or not len(record.msgid):
                            log.debug("empty msgid: %r", record)
                            errors.append( (record.content_hash, "empty msgid") )
                            delete = True
                
                        # Load the message content.
                        try:
                            msg = self.folders[record.folder].get_message(record.msgid, load_content=False)
                
                        except (KeyError, TypeError):
                            log.debug("invalid msgid %s in folder %s", record.msgid, record.folder)
                            errors.append( (record.msgid, "invalid msgid") )
                            delete = True
                        
                        if msg:
                            # Verify that all the flags on the message are in the record.
                            if not set(msg.flags).issubset(set(record.flags)):
                                log.debug("invalid flags: %s", record.msgid)
                                errors.append( (record.msgid, "invalid flags") )
                                record.merge_flags(record.flags)
                                update = True
                                        
                            # Verify that the date is later than the earliest known date.
                            if msg.mtime < record.mtime:
                                log.debug("invalid mtime: %s", record.msgid)
                                errors.append( (record.msgid, "invalid mtime") )
                                record.mtime = msg.mtime
                                update = True
                
                        # Fix anything that needs fixing.
                        if repair:
                            if delete:
                                log.debug("- deleting %r", key)
                                transaction.delete(key)
                                deletes += 1
                            elif update:
                                log.debug("= updating %r", key)
                                transaction.set(key, str(record))
                                updates += 1
                
                        # Print out a status update every once and a while.
                        if delete: mark = 'D'
                        elif update: mark = 'U'
                        else: mark = '.'
                        output.increment(mark)
            
            # Now check the maildirs.
            if handler.STOP == False:
                
                # Cache all the records by their message ID.
                log.debug("Caching index records...")
                records = {}
                for key in self.store.keys():
                    value = self.store[key]
                    record = MailArchiveRecord(value)
                    records[record.msgid] = record
                
                # Iterate over all the folders in the maildir.
                log.warning("* Checking for untracked messages.")
                with self.store as transaction:
                    for foldername in sorted(self.maildir.list_folders()):
                        # Check to see if ^C has been hit.
                        if handler.STOP: break
                
                        folder = self.maildir.get_folder(foldername)
                
                        keys = folder.keys()
                        count = len(keys)
                        interval = max(1, int(count/100))
                
                        # Iterate over all the messages in the folder.
                        log.debug("iterating through %s (%d)", folder.name, count)
                        with Output(name=folder.name + " (check)", total=count) as output:
                            for msgid in sorted(keys):
                                # Check to see if ^C has been hit.
                                if handler.STOP: break
                                
                                # Print a status message every now and again.
                                output.increment('.')
                                
                                msg = folder.get_message(msgid, load_content=True)
                                
                                # Check to see if this message is known in the database or not.
                                if not msgid in records:
                                    errors.append( (msgid, "message in maildir is not in the archive") )
                                    if repair:
                                        if len(msg.content) == 0:
                                            # Delete
                                            log.debug("- delete empty message file")
                                            if msgid in folder: folder.remove(msgid)
                                            deletes += 1
                                            continue
                                    
                                        elif msg.content_hash in transaction:
                                            # Merge and delete
                                            log.debug("= merge duplicate %s", msgid)
                                            self.update_message(msg)
                                            if msgid in folder: folder.remove(msgid)
                                            deletes += 1
                                            updates += 1
                                            record = MailArchiveRecord(transaction[msg.content_hash])
                                    
                                        else:
                                            # Add record
                                            log.debug("+ record for %s", folder._path_for_message(msg))
                                            record = MailArchiveRecord(content_hash=msg.content_hash, mtime=msg.mtime, msgid=msg.msgid, flags=msg.flags, folder=folder.name)
                                            transaction[msg.content_hash] = str(record)
                                            records[msg.content_hash] = record
                                            adds += 1
                                    else:
                                        log.debug("unknown msgid: %s", msgid)
                                
                                else:
                                    record = records[msgid]
                            
                                # Get the cannonical folder for this message.
                                msg_folder = self._folder_for_message(msg)
                                                    
                                # Check to see if it's in that folder.
                                if folder.name != msg_folder.name:
                                    log.debug("~ %s: %s -> %s" % (msgid, folder.name, msg_folder.name))
                                    if repair:
                                        folder.move_message(msgid, msg_folder)
                                        # msg_folder.add_message(msg)
                                        # if msgid in folder: folder.remove(msgid)
                                        record.folder = msg_folder.name
                                        transaction[msg.content_hash] = str(record)
                            
                                # Check the record's folder against (a possibly new) reality.
                                if record.folder != msg_folder.name:
                                    log.debug("~ updating record folder from %s to %s" % (record.folder, msg_folder.name))
                                    if repair:
                                        record.folder = msg_folder.name
                                        transaction[msg.content_hash] = str(record)
                    
        log.warning("* Check complete. %d processed; %d added; %d updated; %d deleted.", count, adds, updates, deletes)
        
        log.debug("* Found %d errors", len(errors))
        
        self.maildir.lazy = was_lazy
        
        return len(errors)
