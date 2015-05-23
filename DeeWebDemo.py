#!/usr/bin/env python
"""DeeWebDemo: web server and front-end for Dee demoCluster"""
__version__ = "0.12"
__author__ = "Greg Gaughan"
__copyright__ = "Copyright (C) 2007 Greg Gaughan"
__license__ = "GPL" #see Licence.txt for licence information

import re
import webbrowser
import mimetypes

from Dee import *

from demoCluster import *

import web  #Public domain: see web.py for details

STATIC_DIRS = ('css', 'js', 'images', 'media')

urls = (
    '/(' + '|'.join(STATIC_DIRS) + ')/.*', 'static',

    '/', 'index',
)


class session:
    def __init__(self):
        self.input=""
        self.output=""
        self.history=[]
        self.history_cursor=len(self.history)

        self.database=demoCluster.values()[0]


sessions = []
nextSessionId = 0

assign_pattern = re.compile("^(\w+)(\s*)(=|\|=|\-=)(\s*)[^=](.*)")

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
                 <font face=verdana,tahoma,arial,helvetica,sans>

                 <h1>%(current_database)s</h1>

                 <form method="post" action="/">
                 <p>
                 <p>Default database:
                 <select name="database_name">%(database)s</select> <input type="submit" name="command" value="Change database" />
                 </p>
                 <input type="submit" name="command" value="<<" />
                 <input type="submit" name="command" value=">>" />
                 <input type="submit" name="command" value="Paste Relation template" />
                 <input type="submit" name="command" value="Paste catalog query" />
                 <br />
                 <label for="expression">Expression:</label><br />
                 <font face=courier>
                 <textarea name="expression" cols=100 rows=10>%(input)s</textarea>
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
        """ % {"current_database":s.database.name,
               "database": "\n".join(['<option value="%(database_name)s" %(selected)s>%(database_name)s' % t for t in demoCluster.databases(['database_name']).extend(['selected'], lambda t:{'selected':t.database_name==s.database.name and "selected" or ""}).toTupleList(sort=(True,['database_name']))]),
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
                    exec(exp, globals(), s.database.transactions[s.database.transactionId])
                    r=""
                except Exception, e:
                    r=e
                    inp=i.expression
            else:
                try:
                    r=eval(exp, globals(), s.database.transactions[s.database.transactionId])
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
                s.input = """relations"""
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
                s.database._dump()
                sys.exit()  #todo better way?
            elif i.command == "Change database":
                s.database = demoCluster[i.database_name]
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
    