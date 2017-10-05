from __future__ import print_function
import optparse as op
import datetime
import sys

#TODO: would be nice to also be able to apply this to the squeue command. However,
#there is no -p option so splitting on spaces will have to be supported.

import logging
logger=logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO
  ,format="%(levelname)s:%(name)s:%(filename)s:%(funcName)s:%(lineno)d:%(message)s")

def addParserOptions(parser):
  """Adds command line options
  """
  
  #parser.add_option("-n",dest="numUsers",type="int"
  #  ,help="Specify the number of users to create/delete [default: %default]."
  #  ,default=1)
  #parser.add_option("--with-hadoop",dest="withHadoop",action="store_true"
  #  ,help="If specified it will also create directories for the users in HDFS [default: %default]."
  #  ,default=False)
  pass
def parseOptions():
  """Parses command line options
  
  """
  
  parser=op.OptionParser(usage="Usage: %prog [options] INPUTFILE"
    ,version="%prog 1.0",description="Calculates queue wait times from a file "
    +"or from stdin generated from running: sacct -p -S  <start-date> -u <username> "
    +"--format=submit,start where <start-date> has the format "
    +"YYYY-MM-DD[THH:MM[:SS]], note [] means contained format is optional or "
    +" from running squeue --Format=submittime,starttime ."
    +"Other format options for sacct/squeue can also be specified but the "
    +"submit/submittime and start/starttime must be present.\n\n"
    +"Commonly used commands are \"sacct -p -S 2017-06-01 -u <username>"
    +" --format=JobIDRaw,state,submit,start,AllocNodes,AllocCPU,MaxRSS,"
    +"partition,account,priority\" and \"squeue -P --sort=-p,i --states=PD "
    +"--Format=jobid,username,account,partition,state,prioritylong\"")
  #squeue -P --sort=-p,i --states=PD --Format=jobid,username,account,partition,state,prioritylong
  #use below command to get a list of pending jobs sorted in the order that the Slurm scheduler considers the jobs in
  #seems like this might depend on both the job priority and also the partition.
  #squeue -P --sort=-p,i --states=PD --Format=jobid,username,account,partition,state,prioritylong >graham_all_pending_jobs_sorted_in_order_of_scheduling_priority.txt
  
  #add options
  addParserOptions(parser)
  
  #parse command line options
  return parser.parse_args()
def getMaxColumnWidths(fileLines,seporator):
  maxColumnWidths=[]
  
  #assume all lines have the same number of columns, should be safe
  
  #Initialize maxColumnWidths with header widths
  line=fileLines[0].strip()
  columns=line.split(seporator)
  for column in columns:
    maxColumnWidths.append(len(column))
  
  #check the rest of the lines
  for line in fileLines:
    columns=line.strip().split(seporator)
    columnIndex=0
    for column in columns:
      lenColumn=len(column.strip())
      if lenColumn>maxColumnWidths[columnIndex]:
        maxColumnWidths[columnIndex]=lenColumn
      columnIndex+=1
  return maxColumnWidths
def printLinePlusQueueWaitTime(line,submitColumn,startColumn,formatString,seperator):
  timeFormat='%Y-%m-%dT%H:%M:%S'
  
  line=line.strip()
  splitLine=line.split(seperator)
  logger.debug("splitLine="+str(splitLine))
  
  #get submit time
  if splitLine[submitColumn]!="Unknown":
    submitTime=datetime.datetime.strptime(splitLine[submitColumn],timeFormat)
  else:
    #TODO: add a message saying time not known for a job
    return None#can't count a difference if the time is unknown
  
  #get start time
  if splitLine[startColumn] not in ["Unknown","N/A"]:
    startTime=datetime.datetime.strptime(splitLine[startColumn],timeFormat)
  elif splitLine[startColumn]=="N/A":#use current date/time
    startTime=datetime.datetime.now()
  else:
    #TODO: add a message saying time not known for a job
    return None#can't count a difference if the time is unknown
  
  waitTimeStr=str(startTime-submitTime)
  splitLine.append(waitTimeStr)
  line=formatString.format(*splitLine)
  print(line)
def printFileLinePlusQueueWaitTimes(file):
  
  fileLines=file.readlines()
  file.close()
  header=fileLines[0]
  
  #check that we have some pipes in header
  seperator="|"
  if '|' not in header:
    #raise Exception("no '|'s found in file header, did you run sacct with the \"-p\" option?")
    #using space as a separator doesn't work very well yet
    print("WARNING: no '|'s found in file header, assuming spaces used instead")
    seperator=None#use default, which seems to do the right thing
  header=header.strip()
  headerColumns=header.split(seperator)
  logger.debug("headerColumns="+str(headerColumns))
  
  #check that we have a JobIDRaw, Submit, and Start columns
  #if "JobIDRaw" not in headerColumns:
  #  raise Exception("no \"JobIDRaw\" column found in header, try adding a --format=JobIDRaw,submit,start")    
  #jobIDColumn=headerColumns.index("JobIDRaw")
  
  #get submit time column header
  submitTimeColumnName="Submit"#sacct
  if submitTimeColumnName not in headerColumns:
    submitTimeColumnName="SUBMIT_TIME"#squeue
    if submitTimeColumnName not in headerColumns:
      raise Exception("no \"Submit\" or \"SUBMIT_TIME\" column found in header"
        +", try adding a --format=submit,start or --Format=submittime,starttime")
  submitColumn=headerColumns.index(submitTimeColumnName)
  
  #get start time column header
  startTimeColumnName="Start"#sacct
  if startTimeColumnName not in headerColumns:
    startTimeColumnName="START_TIME"#squeue
    if submitTimeColumnName not in headerColumns:
      raise Exception("no \"Start\" or \"START_TIME\" column found in header"
        +", try adding a --format=submit,start or --Format=submittime,starttime")
  startColumn=headerColumns.index(startTimeColumnName)
  
  columnWidths=getMaxColumnWidths(fileLines,seperator)
  logger.debug("columnWidths="+str(columnWidths))
  columnWidths.append(20)#width of "QueueWait"+1
  
  columnNum=0
  headerColumns.append("QueueWait")
  logger.debug("headerColumns="+str(headerColumns))
  formatString=""
  for column in headerColumns:
    formatString+="{:>"+str(columnWidths[columnNum])+"} "
    columnNum+=1
  headerOut=formatString.format(*headerColumns)
  print(headerOut)
  line=""
  for columnWidth in columnWidths:
    line+=" "
    for i in range(columnWidth):
      line+="-"
  print(line)
  
  for line in fileLines[1:]:#skip header
    printLinePlusQueueWaitTime(line,submitColumn,startColumn,formatString,seperator)
def main():
  
  #parse command line options
  (options,args)=parseOptions()
  
  if len(args)>1:
    raise Exception("Expected one argument only, the input file to process.")
  elif len(args)==1:
    inputFile=open(args[0],"r")
  else:
    inputFile=sys.stdin
    
  printFileLinePlusQueueWaitTimes(inputFile)
if __name__=="__main__":
  main()