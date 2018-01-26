from __future__ import print_function
import sys
import subprocess
import optparse as op

# Run 'sacct --format=ALL' on the given job and present
# the output in a row-based format instead of column-based
# so it's easier to read.
#
# Ross Dickson, ACENET, 2017-12-21
#

def addParserOptions(parser):
  """Adds command line options
  """
  
  #examples of how to add command line options to parser
  parser.add_option("--max-col-width",dest="width_limit",type="int"
    ,help="Limit the columns to this width [default: %default]."
    ,default=40)
  pass
def parseOptions():
  """Parses command line options
  
  """
  
  parser=op.OptionParser(usage="Usage: %prog [options] JOBID"
    ,version="%prog 1.0",description="Run 'sacct --format=ALL' on the given job"
    +" and present the output in a row-based format instead of column-based so"
    +" it's easier to read.")
  
  #add options
  addParserOptions(parser)
  
  #parse command line options
  return parser.parse_args()
def main():
  
  #parse command line options
  (options,args)=parseOptions()
  
  jobid = args[0]
  
  # Get all accounting fields from sacct.
  # -P for parsable, makes '|' the field separator.
  raw = subprocess.check_output(["sacct", "--format=ALL", "-P", "-j", str(jobid)])
  lines = raw.rstrip().splitlines()
  
  # Tabulate the field names (keys).
  keys = lines[0].strip().split("|")
  field_count = len(keys)
  print("Found", len(lines), "lines")
  
  # Create a list of job steps.
  jobsteps = []
  for line in lines[1:]:
      step = line.strip().split("|")
      assert(field_count == len(step))
      jobsteps.append(step)
  
  step_count = len(jobsteps)
  assert(step_count+1 == len(lines))
  
  # Determine a column width for each step.
  # Limit it to width_limit
  col_fmts = []
  for step in jobsteps:
      max_width = 0
      for field in step:
          max_width = max(max_width, len(field)+1)
      max_width = min(options.width_limit, max_width)
      col_fmt = "{0:"+str(max_width)+"}"
      col_fmts.append(col_fmt)
  
  # Pretty-print it.
  for i in range(field_count):
      print("{0:2} {1:17}:".format(i, keys[i]), end="")
      for rec_index in range(step_count):
          print(col_fmts[rec_index].format(jobsteps[rec_index][i]), end="")
      print()
if __name__=="__main__":
  main()
