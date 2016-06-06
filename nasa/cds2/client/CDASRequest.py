from nasa.cds2.client.CDAS_API import *

server = "localhost"
port = 9000

class CDASDemo:

    @classmethod
    def getTimeseriesDomain( cls, exeReq, levelIndex, id ):
        d = Domain(id=id)
        lat = Axis( id="lat", start=45, end=45, system="values")
        lon = Axis( id="lon", start=30, end=30, system="values")
        lev = Axis( id="lev", start=levelIndex, end=levelIndex, system="indices")
        for axis in [lat,lon,lev]: d.addAxis ( axis )
        exeReq.addDomain( d )
        return d

    @classmethod
    def getSpatialDomain( cls, exeReq, levelIndex, id ):
        d = Domain(id=id)
        time = Axis( id="time", start=4, end=4, system="indices")
        lev = Axis( id="lev", start=levelIndex, end=levelIndex, system="indices")
        for axis in [time,lev]: d.addAxis ( axis )
        exeReq.addDomain( d )
        return d

    @classmethod
    def getColumnDomain( cls, exeReq, id ):
        d = Domain(id=id)
        time = Axis( id="time", start=4, end=4, system="indices")
        lat = Axis( id="lat", start=45, end=45, system="values")
        lon = Axis( id="lon", start=30, end=30, system="values")
        for axis in [time,lat,lon]: d.addAxis ( axis )
        exeReq.addDomain( d )
        return d

    @classmethod
    def getCacheDomain( cls, exeReq, levelIndex, id ):
        d = Domain(id=id)
        lev = Axis( id="lev", start=levelIndex, end=levelIndex, system="indices")
        d.addAxis ( lev )
        exeReq.addDomain( d )
        return d

    @classmethod
    def getMerraAtmosVariable( cls, exeReq, uId, varId, domainId ):
        v = Variable( uid=uId, uri="collection://MERRA/mon/atmos", varname=varId, domain_id=domainId )
        exeReq.addInputVariable( v )
        return v

    @classmethod
    def executeRequest( cls, exeReq, pkg, operation, input_uids, vargs={}, run_async=False ):
        op = Operation( package=pkg, kernel=operation, input_uids=input_uids, args=vargs )
        exeReq.addOperation( op )
        print exeReq.execute( async=run_async )

    @classmethod
    def executeCacheRequest(cls, levelIndex ):
        exeReq = CDASExecuteRequest(server,port)
        cls.getCacheDomain( exeReq, levelIndex, 'dc' )
        cls.getMerraAtmosVariable( exeReq, "v0", "ta", "dc" )
        cls.executeRequest( exeReq, "util", "cache", [ "v0"] )

    @classmethod
    def executeAnomalyConcise(cls, levelIndex):
        cls.executeCacheRequest(levelIndex)
        exeReq = CDASExecuteRequest(server,port)
        cls.getTimeseriesDomain(exeReq, levelIndex, 'd0' )
        cls.getMerraAtmosVariable(exeReq, "v0", "ta", "d0" )
        cls.executeRequest(exeReq, "CDS", "anomaly", [ "v0"], { "axes":"t" } )

    @classmethod
    def executeAnomalyVerbose(cls):
        exeReq = CDASExecuteRequest(server,port)
        v0 = Variable( uid="v0", uri="collection://MERRA/mon/atmos", varname="ta", domain_id="d0" )
        exeReq.addInputVariable( v0 )
        d0 = Domain(id="d0")
        lat = Axis( id="lat", start=45, end=45, system="values")
        lon = Axis( id="lon", start=30, end=30, system="values")
        lev = Axis( id="lev", start=3, end=3, system="indices")
        for axis in [lat,lon,lev]: d0.addAxis ( axis )
        exeReq.addDomain( d0 )
        op = Operation( package="CDS", kernel="anomaly", input_uids=[ "v0"], args={ "axes":"t" } )
        exeReq.addOperation( op )
        print exeReq.execute( async=False )

    @classmethod
    def executeSpatialMax(cls, levelIndex):
        cls.executeCacheRequest(levelIndex)
        exeReq = CDASExecuteRequest(server,port)
        cls.getSpatialDomain(exeReq, levelIndex, 'd0' )
        cls.getMerraAtmosVariable(exeReq, "v0", "ta", "d0" )
        cls.executeRequest(exeReq, "CDS", "max", [ "v0"], { "axes":"xy" } )

    @classmethod
    def executeTemporalMin(cls, levelIndex):
        cls.executeCacheRequest(levelIndex)
        exeReq = CDASExecuteRequest(server,port)
        cls.getTimeseriesDomain(exeReq, levelIndex, 'd0' )
        cls.getMerraAtmosVariable(exeReq, "v0", "ta", "d0" )
        cls.executeRequest(exeReq, "CDS", "min", [ "v0"], { "axes":"t" } )

    @classmethod
    def executeColumnSum(cls):
        exeReq = CDASExecuteRequest(server,port)
        cls.getColumnDomain(exeReq, 'd0' )
        cls.getMerraAtmosVariable(exeReq, "v0", "ta", "d0" )
        cls.executeRequest(exeReq, "CDS", "sum", [ "v0"], { "axes":"z" } )

    @classmethod
    def executeYearlyMeans(cls, levelIndex):
        cls.executeCacheRequest(levelIndex)
        exeReq = CDASExecuteRequest(server,port)
        cls.getTimeseriesDomain( exeReq, levelIndex, 'd0' )
        cls.getMerraAtmosVariable(exeReq, "v0", "ta", "d0" )
        cls.executeRequest(exeReq, "CDS", "timeBin", [ "v0"], {  "period":"12", "unit":"month" } )

    @classmethod
    def executeYearlyCycle(cls, levelIndex):
        cls.executeCacheRequest(levelIndex)
        exeReq = CDASExecuteRequest(server,port)
        cls.getTimeseriesDomain( exeReq, levelIndex, 'd0' )
        cls.getMerraAtmosVariable(exeReq, "v0", "ta", "d0" )
        cls.executeRequest(exeReq, "CDS", "timeBin", [ "v0"], { "period":"1", "unit":"month", "mod":"12" } )

    @classmethod
    def executeSeasonalCycle(cls, levelIndex):
        cls.executeCacheRequest(levelIndex)
        exeReq = CDASExecuteRequest(server,port)
        cls.getTimeseriesDomain( exeReq, levelIndex, 'd0' )
        cls.getMerraAtmosVariable(exeReq, "v0", "ta", "d0" )
        cls.executeRequest(exeReq, "CDS", "timeBin", [ "v0"], { "period":"3", "unit":"month", "mod":"4", "offset":"2"   } )

    @classmethod
    def executeSpatialAve(cls, levelIndex):
        cls.executeCacheRequest(levelIndex)
        exeReq = CDASExecuteRequest(server,port)
        cls.getSpatialDomain( exeReq, levelIndex, 'd0' )
        cls.getMerraAtmosVariable(exeReq, "v0", "ta", "d0" )
        cls.executeRequest(exeReq, "CDS", "average", [ "v0"], { "axes":"xy", "weights": "cosine" } )

    @classmethod
    def getCapabilities(cls):
        exeReq = CDASExecuteRequest(server,port)
        exeReq.getCapabilities()

    @classmethod
    def describeProcess(cls, processName ):
        exeReq = CDASExecuteRequest(server,port)
        exeReq.describeProcess( processName )

    @classmethod
    def listCollections(cls ):
        exeReq = CDASExecuteRequest(server,port)
        cls.executeRequest(exeReq, "CDS", "metadata", [] )

if __name__ == "__main__":
    levelIndex = 3
    CDASDemo.executeColumnSum()


