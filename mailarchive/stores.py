import os
from errno import *


class Store(object):
    '''
    Generic superclass for a key-value store. Implement get/set/delete/keys and the rest is free.
    '''
    def __init__(self, storepath):
        self.storepath = storepath

    def __delitem__(self, key):
        return self.delete(key)
        
    def __setitem__(self, key, value):
        return self.set(key, value)
    
    def __getitem__(self, key):
        return self.get(key)
        
    def __contains__(self, key):
        try:
            v = self.get(key)
            if v != None: return True
        except KeyError:
            pass
        return False
    
    def __iter__(self):
        for key in self.keys():
            yield key
    
    def __len__(self):
        return len(self.keys())
        
    def delete(self, key):
        raise KeyError
    
    def set(self, key, value):
        raise KeyError
    
    def get(self, key):
        raise KeyError
    
    def keys(self):
        return []


import dbm.ndbm as dbm
class DBMStore(Store):
    ext = ".db"
    def __init__(self, storepath):
        if storepath.endswith(self.ext):
            storepath = storepath[:-len(self.ext)]
            
        self.storepath = storepath
        self.db = dbm.open(storepath, "c", 0o600)
    
    def get(self, key):
        return self.db[key].decode("utf8")
    
    def set(self, key, value):
        self.db[key] = value.encode("utf8")
    
    def delete(self, key):
        del self.db[key]
    
    def keys(self):
        return self.db.keys()
    
    def values(self):
        return self.db.values()


import sqlite3
class SQLStore(Store):
    ext = ".sqlite3"
    def __init__(self, storepath):
        if not storepath.endswith(self.ext):
            storepath += self.ext
        
        is_new = not os.path.exists(storepath)
        
        self.db = sqlite3.connect(storepath)
        self.db.isolation_level = None
        
        if is_new:
            self.db.execute("CREATE TABLE kvs (key VARCHAR(255) NOT NULL PRIMARY KEY, value TEXT)")
    
    def __enter__(self):
        self.db.execute("BEGIN TRANSACTION")
        return self
        
    def __exit__(self, *args):
        #FIXME: ROLLBACK on exceptions
        self.db.execute("COMMIT")
    
    def get(self, key):
            c = self.db.execute("SELECT value FROM kvs WHERE key = ?", (key,))
            value = c.fetchone()
            if value:
                return value[0]
            else:
                raise KeyError
    
    def set(self, key, value):
            try:
                self.db.execute("INSERT INTO kvs VALUES (?, ?)", (key, value))
            except sqlite3.IntegrityError:
                self.db.execute("UPDATE kvs SET value = ? WHERE key = ?", (value, key))
    
    def delete(self, key):
            self.db.execute("DELETE FROM kvs WHERE key = ?", (key,))
    
    def keys(self):
            c = self.db.execute("SELECT key FROM kvs")
            result = c.fetchall()
            result = [x[0] for x in result] #unpack from per-row
            return result
    
    def values(self):
            c = self.db.execute("SELECT value FROM kvs")
            result = c.fetchall()
            result = [x[0] for x in result] #unpack from per-row
            return result


class SymStore(Store):
    """
    A key-value store using symlinks.
    
    Mostly a joke/challenge, but who knows what someone could do
    with it?
    """
    
    ext = ".symdb"
    def __init__(self, storepath):
        if not storepath.endswith(self.ext):
            storepath += self.ext
        self._cache_age = 0
        self._key_cache = []
        self.storepath = storepath
        if not os.path.exists(self.storepath):
            os.makedirs(self.storepath, mode=0o700, exist_ok=True)
            
    def get(self, key):
        try:
            value = os.readlink(os.path.join(self.storepath, key))
            return value
        except OSError as e:
            if e.errno != ENOENT: raise
            return None
    
    def set(self, key, value):
        try:
            os.symlink(value, os.path.join(self.storepath, key))
            
        except OSError as e:
            if e.errno == EEXIST:
                raise KeyError
            else:
                raise
            
        return key
    
    def delete(self, key):
        try:
            os.remove(os.path.join(self.storepath, key))
        except OSError as e:
            if e.errno == ENOENT:
                raise KeyError
            else:
                raise
    
    def keys(self):
        mtime = os.path.getmtime(self.storepath)
        if mtime > self._cache_age:
            self._key_cache = os.listdir(self.storepath)
            self._cache_age = mtime
        return self._key_cache
