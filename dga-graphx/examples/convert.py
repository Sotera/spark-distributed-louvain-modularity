import sys

DEL = '\t'

if __name__ == '__main__':
  if len(sys.argv) != 3:
    print 'usage: ',sys.argv[0],' <input> <output>'
    sys.exit(1)

  nodes = {}
  curr = 0
  fobj = open(sys.argv[1],'r')
  for line in fobj:
    line = line.strip().split(DEL)
    if line[0] not in nodes:
      nodes[line[0]] = curr
      curr +=1
    if line[1] not in nodes:
      nodes[line[1]] = curr
      curr += 1

  fobj.close()
  print 'Highest label give: ',curr
  fobj = open(sys.argv[1],'r')
  out =  open(sys.argv[2],'w')

  for line in fobj:
    line = line.strip().split(DEL)
    out.write(str(nodes[line[0]])+DEL+str(nodes[line[1]])+'\n')

  fobj.close()
  out.close()
