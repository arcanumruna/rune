#!/usr/bin/env python3
"""frwaterfall.py — replication commit/ack latency from rune-fr Chrome traces.
Truncation-tolerant: regex-extracts complete event objects (recovers the valid prefix
if kill -9 cut the trailing ']'), so it works even on a mid-write snapshot.
Pairs same-process marks by corr=(partition<<24)|offset on each node:
  commit = ReplHwAdvance.ts  - ReplLeaderAppend.ts   (append -> majority-committed)
  ack    = ReplAckSendOff.ts - ReplLeaderAppend.ts   (append -> client ack enqueued)
Usage: frwaterfall.py <fr-trace-*.json ...>
"""
import re, sys, glob
EV = re.compile(r'"name":"([^"]+)".*?"ts":([\d.]+),"dur":([\d.]+),"args":\{"corr":(\d+)')
def pct(xs,p):
    if not xs: return 0.0
    s=sorted(xs); return s[min(len(s)-1,int(p/100.0*len(s)))]
files=[]
for a in sys.argv[1:]: files+=glob.glob(a)
if not files: print("no trace files"); sys.exit(1)
commit,ack={},{}
commit_us,ack_us=[],[]
span={}
for f in files:
    appends,hw,acks={},{},{}
    data=open(f,'r',errors='replace').read()
    n=0
    for m in EV.finditer(data):
        name,ts,dur,corr=m.group(1),float(m.group(2)),float(m.group(3)),int(m.group(4)); n+=1
        if dur>0: span.setdefault(name,[]).append(dur)
        if name=="ReplLeaderAppend": appends[corr]=ts
        elif name=="ReplHwAdvance": hw[corr]=ts
        elif name=="ReplAckSendOff": acks[corr]=ts
    for c,t in appends.items():
        if c in hw and hw[c]>=t: commit_us.append(hw[c]-t)
        if c in acks and acks[c]>=t: ack_us.append(acks[c]-t)
    print(f"  parsed {n} events from {f.split('/')[-1]}")
def line(lbl,xs):
    if not xs: print(f"  {lbl:<26} (no paired samples)"); return
    print(f"  {lbl:<26} n={len(xs):<7} p50={pct(xs,50):9.2f}us p99={pct(xs,99):10.2f}us max={max(xs):11.2f}us mean={sum(xs)/len(xs):9.2f}us")
print("\n=== REPLICATION COMMIT / ACK LATENCY (leader same-process marks, all nodes) ===")
line("append->HW-commit",commit_us)
line("append->client-ack",ack_us)
print("\n=== PER-STAGE SPAN durations (dur>0), all nodes ===")
for s in sorted(span,key=lambda k:-pct(span[k],50)):
    xs=span[s]; print(f"  {s:<22} n={len(xs):<7} p50={pct(xs,50):9.2f}us p99={pct(xs,99):10.2f}us max={max(xs):11.2f}us")
