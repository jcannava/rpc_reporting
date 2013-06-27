import os
import pyrax
import sqlite3
import gzip

# Setup database
database = 'logs.sqlite'
table = 'log_metadata'
conn = sqlite3.connect(database)
c = conn.cursor()
sql = 'create table if not exists '\
      + table + ' (id INTEGER PRIMARY KEY, date text, filename text UNIQUE)'
c.execute(sql)
conn.commit()

# Set Credentials
pyrax.set_setting("identity_type", "rackspace")
pyrax.set_setting("region", "ORD")
pyrax.set_credential_file(".pyrax.cfg")

# Setup cloudfiles
cf = pyrax.cloudfiles

# Get Access Log Container for V4 (change later if this changes)
cont = cf.get_container(".CDN_ACCESS_LOGS")
print cont

# Get list of logs in container
objects = cont.get_objects()

# download objects, checking that they have not already been downloaded
for obj in objects:
    cont, yr, mo, day, hour, fname = obj.name.split('/')
    date = yr + "/" + mo + "/" + day
    sql = 'insert or ignore into ' + table +\
          ' (date,filename) values ("%s","%s")' % (date, fname)
    c.execute(sql)
    conn.commit()
    obj_path = "logs/" + fname

    if not os.path.exists(obj_path):
        file = open(obj_path, "w")
        try:
            file.write(obj.get())
        except:
            print "Cannot write file", obj_path
        finally:
            file.close()

    # Setup for decompressing gzip files
    uuid, ext, suffix = fname.split('.')
    outfile = "logs/" + uuid + "." + ext

    # Decompress log file
    infile = gzip.GzipFile(obj_path, "rb")
    s = infile.read()
    infile.close
    out = open(outfile, "wb")
    out.write(s)
    out.close()
    os.remove(obj_path)
