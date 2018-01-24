#!/usr/bin/python
# vim: tabstop=4 softtabstop=4 shiftwidth=4 expandtab
# Author: Jacek Budzowski <j.budzowski@cyfronet.pl>
#
__version__="0.1 [2017-11-16]"

import subprocess, sys, datetime, argparse, StringIO, gzip

SLURM_BIN_PATH="/mnt/nfs/slurm/releases/production/bin"
DEFAULT_LOGPATH="/mnt/nfs/slurm/accounting/apel"

def error(inp=""):
    print >>sys.stderr, inp

def debug(inp=""):
    if debug_mode:
        print >>sys.stderr, inp

# Wrapper for subprocess library
def subprocessCall(*call):
    try:
        process=subprocess.Popen(list(call), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out,err=process.communicate()
    except KeyboardInterrupt:
        sys.exit(0)
    code=process.returncode
    return ({'code':code,'stdout':out,'stderr':err})

# Get SLURM commands output and check its sanity
def getOutput(*call):
    try:
        debug=" ".join(call)
        data=subprocessCall(*call)
        if 'rc = Invalid user' in data["stderr"]:
            error("Permission error")
            sys.exit(1)
        elif 'Invalid user' in data["stderr"]: 
            error("Invalid user")
            sys.exit(2)
        return data["stdout"].splitlines()
    except subprocess.CalledProcessError as e:
        error("%s() error"%call[0])
        error("subprocess returned data: %s"%('\n'.join([data["stdout"],data["stderr"]])))
        error("Exception: %s"%e)
        error("Error no. 1")
        sys.exit(1)

# Sacct command call
def getSacct(*args):
    data=getOutput(SLURM_BIN_PATH + "/sacct", *args)
    return data

# Get yesterday's date
def getYesterday():
    return str(datetime.date.today()-datetime.timedelta(days=1))

# Check if given date is valid
def dateParser(inp):
    try:
        date=datetime.datetime.strptime(inp,"%Y-%m-%d")
        return datetime.date.strftime(date,"%Y-%m-%d")
    except:
        raise argparse.ArgumentTypeError("Bad date specified: %s"%(inp))

# Start here
if __name__=="__main__":

    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter, description="{progname} v.{version}\n\nGet SLURM jobs data for APEL parser.\nUncompressed data from yesterday is printed on stdout by default.\n".format(progname=sys.argv[0].split("/")[-1].upper(), version=__version__) ,epilog="---")
    parser.add_argument('-c','--compress','-g','--gzip',action="store_true", dest="compress_mode", help="compress (gzip) data")
    parser.add_argument('-d','--day','--date',action="store", metavar='<DDDD-MM-YY>', dest="day", type=dateParser, default=getYesterday(), help="specify day of logs")
    parser.add_argument('-D','--debug',action="store_true", dest="debug_mode", help="debug mode")
    parser.add_argument('-s','--save','--save-to-files',action="store", metavar="PATH", nargs="*", dest="save_path", help="save data to logfile")
    parser.add_argument('-V','--version', action='version', version='%(prog)s {version}'.format(version=__version__))
    parser=parser.parse_args()
    #
    compress_mode=parser.compress_mode
    day=parser.day
    debug_mode=parser.debug_mode
    save_path=parser.save_path
    if save_path==None:
        save_mode=False
    elif save_path:
        save_mode=True
        save_path=save_path[0]
    else:
        save_mode=True
        save_path=DEFAULT_LOGPATH

    logfilename="%s/%s%s"%(save_path,day.translate(None,"-"),".gz" if compress_mode else "")
    debug("Started processing file: %s"%(logfilename))

    sacct_data=getSacct("--parsable2","--noheader","--duplicates","--state=COMPLETED,TIMEOUT","-S","%sT00:00:00"%(day),"-E","%sT23:59:59"%(day),"--format=JobID,JobName,User,Group,Start,End,Elapsed,CPUTimeRAW,Partition,NCPUS,NNodes,NodeList,MaxRSS,MaxVMSize,State")
#['301088.batch|batch|||2017-11-09T17:14:13|2017-11-15T23:14:37|6-06:00:24|17280768||32|1|n1047-amd|1599228K|186180K|CANCELLED', '301088.0|orted|||2017-11-09T17:14:15|2017-11-15T23:14:38|6-06:00:23|1080046||2|2|n1059-amd,n1062-amd|1594708K|212400K|FAILED', '316680|crm01_513142467|pltlhcb005|lhcbplt|2017-11-12T00:03:58|2017-11-15T00:04:19|3-00:00:21|259221|grid-lhcb|1|1|n1068-amd|||TIMEOUT', '316680.batch|batch|||2017-11-12T00:03:58|2017-11-15T00:07:20|3-00:03:22|259402||1|1|n1068-amd|1402708K|784208K|CANCELLED', '316687|crm01_106986107|pltlhcb005|lhcbplt|2017-11-12T00:05:07|2017-11-15T00:05:19|3-00:00:12|259212|grid-lhcb|1|1|n1086-amd|||TIMEOUT', '316687.batch|batch|||2017-11-12T00:05:07|2017-11-15T00:08:21|3-00:03:14|259394||1|1|n1086-amd|1807144K|2518200K|CANCELLED', '316707|crm01_403872042|pltlhcb005|lhcbplt|2017-11-12T00:11:01|2017-11-15T00:11:19|3-00:00:18|259218|grid-lhcb|1|1|n1086-amd|||TIMEOUT', '316707.batch|batch|||2017-11-12T00:11:01|2017-11-15T00:14:21|3-00:03:20|259400||1|1|n1086-amd|1468040K|510140K|CANCELLED', '316758|crm01_681680768|pltlhcb005|lhcbplt|2017-11-12T00:12:07|2017-11-15T00:12:19|3-00:00:12|259212|grid-lhcb|1|1|n1075-amd|||TIMEOUT']

    jobs={}
    for entry in sacct_data:
        job_entry=entry.split("|")
        jobid=job_entry[0].split(".")[0]
        jobname=job_entry[1]
        jobuser=job_entry[2]
        if jobid not in jobs and jobname.startswith("crm01_") and not jobuser.startswith("plg"):
            jobs[jobid]=job_entry
        #set rss anv maxmvsize from job step
        elif jobid in jobs:
            rss=job_entry[12]
            vmsize=job_entry[13]
            jobs[jobid][12]="" if rss == "0" else rss
            jobs[jobid][13]="" if vmsize == "0" else vmsize

    output_data="\n".join(["|".join(jobs[jobid]) for jobid in jobs])

    if compress_mode:
        out=StringIO.StringIO()
        gzipped=gzip.GzipFile(fileobj=out, mode="w")
        gzipped.write(output_data)
        gzipped.flush()
        gzipped.close()
        out.flush()
        output_data=out.getvalue()
        out.close()
    if save_mode:
        try:
            logfile=file(logfilename,"w")
        except IOError:
            error("Unable to write data to logfile: %s"%(logfilename))
            sys.exit(1)
        logfile.write(output_data)
        logfile.close()
        debug("Finished processing file: %s"%(logfilename))
    else:
        try:
            print output_data
        except IOError:
            pass

    sys.exit(0)
