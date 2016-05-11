import urllib2
import xml.dom.minidom

class ResultVariable:

    def __init__( self, dom ):
        self._dom = dom

    def __str__(self): return self._dom.toprettyxml()

    def axes(self): return self._dom.getElementsByTagName('axis')

def execRequest( requestURL ): return ResultVariable( xml.dom.minidom.parseString( urllib2.urlopen( requestURL ).read() ) )

class Variable:
    def __init__( self, uid, uri, varname, domain_id ):
        self._uid = uid
        self._uri = uri
        self._varname = varname
        self._dom_id = domain_id

    def toWps(self): return '{"uri":"%s","name":"%s:%s","domain":"%s"}' % ( self._uri, self._varname, self._uid, self._dom_id )

class Domain:
    def __init__( self, id, axes=[] ):
        self._id = id
        self._axes = axes

    def addAxis( self,  a ): self._axes.append( a )

    def toWps(self):
        return '{"name":"%s",%s}' % ( self._id, ",".join([axis.toWps() for axis in self._axes]) )

class Axis:
    def __init__( self, id, start, end, system ):
        self._id = id
        self._start = start
        self._end = end
        self._system = system

    def toWps(self): return '"%s":{"start":%s,"end":%s,"system":"%s"}' % ( self._id, self._start, self._end, self._system )

class Operation:

    def __init__( self, package, kernel, input_uids, args, result_id = None ):
        self._result_id = result_id
        self._package = package
        self._kernel = kernel
        self._args = args
        self._input_uids = input_uids

    def getIdentifier(self): return '%s.%s' % ( self._package, self._kernel )

    def toWps(self):
        call_signature = ( '%s:' % ( self._result_id ) if self._result_id else '' ) + self.getIdentifier()
        inputs = ",".join(self._input_uids)
        args = ",".join( [ ('%s:%s' % ( key, value )) for key, value in self._args.iteritems()] )
        return "%s(%s,%s)" % ( call_signature, inputs, args )

def boolStr( bval ): return "true" if bval else "false"

class CDASExecuteRequest:

    def __init__( self, server, port ):
        self._server = server
        self._port = port
        self._variables = []
        self._domains = []
        self._operations = []

    def _getBaseRequest( self, async ): return 'http://%s:%s/wps?request=Execute&service=cds2&status=%s' % ( self._server, self._port, boolStr(async) )

    def _getIdentifier(self ):
        if   len( self._operations ) == 0: return '&identifier=util.cache'
        elif len( self._operations ) == 1: return ( '&identifier=%s' % self._operations[0].getIdentifier() )
        else: return ( '&identifier=CDS.workflow' )

    def _getDatainputs(self ):
        return '&datainputs=[%s,%s,%s]' % ( self._getDomains(), self._getVariables(), self._getOperations() )

    def _getDomains( self ):
        return 'domain=[%s]' % ( ",".join( [ domain.toWps() for domain in self._domains ] )  )

    def _getVariables( self ):
        return 'variable=[%s]' % ( ",".join( [ variable.toWps() for variable in self._variables ] )  )

    def _getOperations( self ):
        return 'operation="%s"' % ( ",".join( [ operation.toWps() for operation in self._operations ] )  )

    def addInputVariable( self, v ): self._variables.append( v )

    def addDomain( self, d ): self._domains.append( d )

    def addOperation(self, o ): self._operations.append( o )

    def toWps( self, async ):
        return self._getBaseRequest( async ) + self._getIdentifier( ) + self._getDatainputs( )

    def execute( self, async ):
        request = self.toWps(async)
        print "\nExecuting Request:\n\n%s\n\nResponse:\n" % ( request )
        return execRequest( request )
