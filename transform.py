import csv
from math import exp, log

# DMC: Duff Moisture Code
# DC: Drought Code
def calc_bui(dmc: float, dc: float) -> float:
  if dmc <= 0.4 * dc:
    return 0.8 * dmc * dc / (dmc + 0.4 * dc)
  else:
    return dmc - (1. - 0.8 * dc / (dmc + 0.4 * dc)) * (0.92 + (0.0114 * dmc) ** 1.7)

# ISI: Initial Spread Index
# BUI: Buildup Index
def calc_fwi(isi: float, bui: float) -> float:
  if bui <= 80.:
    fd = 0.626 * bui ** 0.809 + 2
  else:
    fd = 1000 / (25 + 108.64 * exp(-0.023 * bui))

  B = 0.1 * isi * fd
  if B <= 1:
    return B
  else:
    return exp(2.72 * (0.434 * log(B)) ** 0.647)

with open('forestfires.csv', 'r') as csvinput:
  with open('output.csv', 'w') as csvoutput:
    reader = csv.reader(csvinput)
    writer = csv.writer(csvoutput, lineterminator='\n')
    
    header = next(reader)
    header.append('BUI')
    header.append('FWI')
    writer.writerow(header)

    for row in reader:
      bui = round(calc_bui(float(row[5]), float(row[6])), 2)
      fwi = round(calc_fwi(float(row[7]), bui), 2)
      row.append(bui)
      row.append(fwi)
      writer.writerow(row)


