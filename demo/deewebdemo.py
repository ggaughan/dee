#!/usr/bin/env python
"""deewebdemo: web server and front-end for Dee demo"""
__version__ = "0.2"
__author__ = "Greg Gaughan"
__copyright__ = "Copyright (C) 2007 Greg Gaughan"
__license__ = "GPL" #see Licence.txt for licence information

import re
import webbrowser
import mimetypes

from dee.relation import *

import date_db
import darwen_db

databases = {'Date': date_db.db,
             'Darwen': darwen_db.db,
            }

import web  #Public domain: see web.py for details

STATIC_DIRS = ('css', 'js', 'images', 'media')

urls = (
    '/(' + '|'.join(STATIC_DIRS) + ')/.*', 'static',

    '/', 'index',
)


class session():
    def __init__(self):
        self.input=""
        self.output=""
        self.history=[]
        self.history_cursor=len(self.history)

        self.current_database = "Date"
        self.database = databases[self.current_database]


sessions = []
nextSessionId = 0

assign_pattern = re.compile("^([\w|\.]+)(\s*)(=|\|=|\-=)(\s*)[^=](.*)")

def getSession():
    global nextSessionId

    res = None
    sessionref = web.cookies()

    #web.debugwrite("Before:"+str(sessions))
    if sessionref:
        try:
            web.debugwrite("Using existing session %s" % sessionref.id)
            res = sessions[int(sessionref.id)]
        except:
            web.debugwrite(" - session no longer valid")
            pass
    if not res:
        web.debugwrite("Creating new session %s" % nextSessionId)
        if len(sessions) == nextSessionId:
            sessions.append(session())
        else:
            assert False, "Sessions out of sync. with nextSessionId"
        res = sessions[nextSessionId]
        web.setcookie('id', nextSessionId)
        nextSessionId += 1  #todo random!
        #web.debugwrite("After:"+str(sessions))

    return res


class index:
    def GET(self):
        s = getSession()

        print """<html>
                 <head>
                 <title>Dee</title>
                 <link rel="stylesheet" type="text/css" href="css/plainold.css" media="screen"/>
                 </head>

                 <body>

                 <h1>%(current_database)s</h1>

                 <form method="post" action="/">
                 <p>
                 <p>Default database (db.):
                 <select name="database_name">%(database)s</select> <input type="submit" name="command" value="Change database" />
                 </p>
                 <input type="submit" name="command" value="<<" />
                 <input type="submit" name="command" value=">>" />
                 <input type="submit" name="command" value="Paste Relation template" />
                 <input type="submit" name="command" value="Paste catalog query" />
                 <br />
                 <label for="expression">Expression:</label><br />
                 <font face=courier>
                 <textarea name="expression" cols=80 rows=10 autofocus="autofocus">%(input)s</textarea>
                 </font>
                 <input type="submit" name="command" value="Evaluate" />
                 </p>
                 <p>
                 <font face=courier>
                 %(output)s
                 </font>
                 </p>
                 </form>

                 </font>
                 </body>

                 </html>
        """ % {"current_database":s.current_database,
               "database": "\n".join(['<option value="%(database_name)s" %(selected)s>%(database_name)s' % {'database_name':t, 'selected':t==s.current_database and "selected" or ""} for t in databases]),
               "input":s.input, "output":s.output}

    def POST(self):
        s = getSession()

        i = web.input()

        if i.command == "Evaluate":
            inp = ""
            exp = i.expression.rstrip()
            s.history.append(exp)
            s.history_cursor=len(s.history)

            exp = exp.replace('\n', ' ').replace('\r', '')
            
            if assign_pattern.match(exp):
                try:
                    exec(exp, globals(), {'db':s.database})
                    r=""
                except Exception, e:
                    r=e
                    inp=i.expression
            else:
                try:
                    r=eval(exp, globals(), {'db':s.database})
                    if isinstance(r, Relation):
                        r="""<div id="itsthetable">%s</div>""" % r.renderHTML()
                    else:
                        r=str(web.websafe(r))
                except Exception, e:
                    r=e
                    inp=i.expression
            s.input = inp
            s.output = "<b>&gt;&gt;&gt; %s</b><br />%s<br />%s" % (exp, r, s.output)
            web.redirect('/')
        else:
            if i.command == "Paste Relation template":
                s.input = """Relation(["a", "b"],
        [('one', 1),
         ('two', 2),
         ('three', 3),
        ])"""
            elif i.command == "Paste catalog query":
                s.input = """db.catalog.relvars"""
            elif i.command == "<<":
                if s.history_cursor>0:
                    s.history_cursor-=1
                    s.input = s.history[s.history_cursor]
                else:
                    s.input = i.expression
            elif i.command == ">>":
                if s.history_cursor < len(s.history)-1:
                    s.history_cursor+=1
                    s.input = s.history[s.history_cursor]
                else:
                    s.input = i.expression
            elif i.command == "Shutdown":
                #todo s.database._dump()
                sys.exit()  #todo better way?
            elif i.command == "Change database":
                s.current_database = i.database_name
                s.database = databases[s.current_database]
            else:
                assert False, "Unexpected command"

            web.redirect('/')
            return


def mime_type(filename):
    return mimetypes.guess_type(filename)[0] or 'application/octet-stream'

class static:
    def GET(self, static_dir=''):
        try:
            static_file_name = web.context.path.split('/')[-1]
            web.header('Content-type', mime_type(static_file_name))
            static_file = open('.' + web.context.path, 'rb')
            web.ctx.output = static_file
        except IOError:
            web.notfound()


# For debugging use only
web.internalerror = web.debugerror

if __name__ == "__main__":
    open("startPage.html", 'w').write("""
    <html>
    <head>
    <title>Starting</title>
    </head>
    <body>
    <meta HTTP-EQUIV="Refresh" CONTENT="1; URL=http://127.0.0.1:8080">
    <h1 align="center">Starting</h1>
    </body>
    </html>
    """)

    try:
        webbrowser.open("startPage.html", new=0, autoraise=1)
    except:
        print "Point your browser at http://localhost:8080"


    web.run(urls, web.reloader)
    