#!/bin/env python
from __future__ import print_function
import optparse as op
import datetime
import sys

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
    +"YYYY-MM-DD[THH:MM[:SS]], note [] means contained format is optional. "
    +"Other format options for sacct can also be specified but the submit and "
    +"start times must be present.\n\n"
    +"Commonly used command is sacct -p -S 2017-06-01 -u <username>"
    +" --format=JobIDRaw,state,submit,start,AllocNodes,AllocCPU,MaxRSS,partition,account,priority")

  #use below command to get a list of pending jobs sorted in the order that the Slurm scheduler considers the jobs in
  #seems like this might depend on both the job priority and also the partition.
  #squeue -P --sort=-p,i --states=PD --Format=jobid,username,account,partition,state,prioritylong >graham_all_pending_jobs_sorted_in_order_of_scheduling_priority.txt
  
  #add options
  addParserOptions(parser)
  
  #parse command line options
  return parser.parse_args()
def getMaxColumnWidths(fileLines):
  maxColumnWidths=[]
  
  #assume all lines have the same number of columns, should be safe
  
  #Initialize maxColumnWidths with header widths
  columns=fileLines[0].split('|')[:-1]
  for column in columns:
    maxColumnWidths.append(len(column))
  
  #check the rest of the lines
  for line in fileLines:
    columns=line.split('|')[:-1]
    columnIndex=0
    for column in columns:
      lenColumn=len(column.strip())
      if lenColumn>maxColumnWidths[columnIndex]:
        maxColumnWidths[columnIndex]=lenColumn
      columnIndex+=1
  return maxColumnWidths
def printLinePlusQueueWaitTime(line,submitColumn,startColumn,formatString):
  timeFormat='%Y-%m-%dT%H:%M:%S'
  
  splitLine=line.split('|')[:-1]#remove \n at end of list
  logger.debug("splitLine="+str(splitLine))
  
  #get submit time
  if splitLine[submitColumn]!="Unknown":
    submitTime=datetime.datetime.strptime(splitLine[submitColumn],timeFormat)
  else:
    #TODO: add a message saying time not known for a job
    return None#can't count a difference if the time is unknown
  
  #get start time
  if splitLine[startColumn]!="Unknown":
    startTime=datetime.datetime.strptime(splitLine[startColumn],timeFormat)
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
  if '|' not in header:
    raise Exception("no '|'s found in file header, did you run sacct with the \"-p\" option?")
  headerColumns=header.split('|')[:-1]
  
  #check that we have a JobIDRaw, Submit, and Start columns
  if "JobIDRaw" not in headerColumns:
    raise Exception("no \"JobIDRaw\" column found in header, try adding a --format=JobIDRaw,submit,start")    
  jobIDColumn=headerColumns.index("JobIDRaw")
  if "Submit" not in headerColumns:
    raise Exception("no \"Submit\" column found in header, try adding a --format=JobIDRaw,submit,start")
  submitColumn=headerColumns.index("Submit")
  if "Start" not in headerColumns:
    raise Exception("no \"Start\" column found in header, try adding a --format=JobIDRaw,submit,start")    
  startColumn=headerColumns.index("Start")
  
  columnWidths=getMaxColumnWidths(fileLines)
  logger.debug("columnWidths="+str(columnWidths))
  columnWidths.append(20)#width of "QueueWait"+1
  
  columnNum=0
  headerColumns.append("QueueWait")
  logger.debug("headerColumns="+str(headerColumns))
  formatString=""
  for column in headerColumns:
    formatString+="{:>"+str(columnWidths[columnNum]+1)+"}"
    columnNum+=1
  headerOut=formatString.format(*headerColumns)
  print(headerOut)
  
  for line in fileLines[1:]:#skip header
    printLinePlusQueueWaitTime(line,submitColumn,startColumn,formatString)
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