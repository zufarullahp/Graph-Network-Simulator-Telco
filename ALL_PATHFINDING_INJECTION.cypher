
== ALL PATHFINDING INJECTION
ONLY RUN THIS ONCE FOR DATA INGESTION IN PART 1 

== CREATE PF
[source,cypher]
----
MATCH (a)-[r:LINKS]->(b)
WHERE a<>b
MERGE (a)-[rr:PF]->(b)
SET rr = properties(r)
----

[source,cypher]
----
MATCH (a)-[r:LINKS]->(b)
WHERE a<>b
MERGE (a)<-[rr:PF]-(b)
SET rr = properties(r)
----

[source,cypher]
----
MATCH ()-[r:PF]->()
set r.costLink = toInteger(r.costLink)
----

== START PROCESS PATHFINDING (WITHOUT OCCUPANCY)

REMOVE TIERS
[source,cypher]
----
MATCH (n:ME)
REMOVE n:TIER1, n:TIER2, n:TIER3
----

Create TIER1 Label
[source,cypher]
----
MATCH (a:ME)-[r:PF]-(b:BRAS)
SET a:TIER1
----

CREATE TIER2 LABEL
[source,cypher]
----
MATCH (a:ME)-[:PF]-(:TIER1)
WHERE not a:TIER1
SET a:TIER2
----

CREATE TIER3 LABEL
[source,cypher]
----
MATCH p=(n:ME)
WHERE none(a in nodes(p) WHERE a:TIER1 or a:TIER2)
SET n:TIER3
----

== UL DEST1, FAST QUERY, DIRECTED ()
if got this result 
Set 9207 properties, completed after 55079 ms. 
then valid 
[source,cypher]
----
CALL {
  MATCH (a:TRAFFIC_UP)
  WITH a
  CALL {
    WITH a
    MATCH p=((from1:OLT {nameReal: a.sourceReal})-[:PF*2..4]->(:TIER1)-[:PF]->(:BRAS)-[:PF]->(to1:PE))
    WITH from1, to1, a, p
    , [z in nodes(p) WHERE z:TIER1 | z.nameReal] AS TIER1
    , [z in nodes(p) WHERE z:TIER2 | z.nameReal] AS TIER2
    , [z in nodes(p) WHERE z:TIER3 | z.nameReal] AS TIER3
    , [z in nodes(p) WHERE z:CDN | z.nameReal] AS CDN
    , [z in nodes(p) WHERE z:TERA | z.nameReal] AS TERA
    , [z in tail(nodes(p)) WHERE z:OLT | z.nameReal] AS OLT
    , [z in nodes(p) | z][0..size([z in nodes(p) | z])-1] AS listSize
    WHERE 
     none(z in (listSize) WHERE z.nameReal contains 'Google')
     AND size(TIER1)=1
     AND size(CDN)=0
     AND size(TERA)=0
     AND size(OLT)=0
    WITH DISTINCT from1, to1, a, p,
    [z in nodes(p)| z] AS nodes,
    [z in relationships(p)| z] AS rels,
    [z in nodes(p)| z.nameReal] AS nodesName,
    size([z in nodes(p)| z])-1 AS Hops,
    apoc.coll.sum([z in relationships(p) | z.costLink]) AS Costs,
    [z in relationships(p) | z.cap] AS Capacity
    RETURN from1.nameReal AS Source, to1.nameReal AS Target, nodesName, Costs, Capacity, Hops, nodes, rels
    ORDER BY Hops, Costs asc
    LIMIT 1
  }
  WITH a, Source, Target, nodesName, Costs, Hops
  RETURN a, Source, Target, nodesName, Hops, Costs
  ORDER BY Source, Target
}
WITH a, nodesName, Costs, Hops
SET a.dest1=nodesName, a.cost1=Costs, a.hop1=Hops
----

== UL DEST2, FAST QUERY, DIRECTED ()
if got this result 
Set 9207 properties, completed after 4015 ms
then valid 

[source,cypher]
----
CALL {
  MATCH (a:TRAFFIC_UP)
  WITH a
  CALL {
    WITH a
    MATCH p=((from1:allNodes {nameReal: last(a.dest1)})-[:PF*1..4]->(to1:allNodes {nameReal: a.targetReal}))
    WITH from1, to1, a, p, 
      [z in nodes(p) | z][0..size([z in nodes(p) | z])-1] AS listSize
    WHERE none(z in (listSize) WHERE z:CGW or z:CDN)
    WITH distinct from1, to1, a, p,
      [z in nodes(p)| z] AS nodes,
      [z in relationships(p)| z] AS rels,
      [z in nodes(p)| z.nameReal] AS nodesName,
      size([z in nodes(p)| z])-1 AS Hops,
      apoc.coll.sum([z in relationships(p) | z.costLink]) AS Costs,
      [z in relationships(p) | z.cap] AS Capacity
    RETURN from1.nameReal AS Source, to1.nameReal AS Target, nodesName, Costs, Capacity, Hops, nodes, rels
    ORDER BY Hops, Costs asc
    LIMIT 1
  }
  WITH a, Source, Target, nodesName, Costs, Hops
  RETURN a, Source, Target, nodesName, Hops, Costs
  ORDER BY Source, Target
}
WITH a, nodesName, Costs, Hops
SET a.dest2=nodesName, a.cost2=Costs, a.hop2=Hops
----

== UL PREPARE DL REG

create a bridge as property for downlink 

[source,cypher]
----
MATCH (a:TRAFFIC_UP)
SET a.dest = (a.dest1)+tail(a.dest2), a.cost=(a.cost1)+(a.cost2), a.hop=(a.hop1)+(a.hop2),
  a.reg=last(a.dest1),
  a.reg1=case when last(a.dest1) contains 'RKT' then 'MET9**RKT'
         when last(a.dest1) contains 'KBL' then 'MET9**KBL' end
WITH a
MATCH (m:TRAFFIC_DOWN {targetReal: a.sourceReal})
WITH m,a
SET m.reg=a.reg, m.reg1=a.reg1
----

== DL DEST1, FAST QUERY, DIRECTED ()
[source,cypher]
----
CALL {
  MATCH (a:TRAFFIC_DOWN)
  WITH a
  , case
         when a.sourceReal contains 'CGW' then a.reg
         when a.sourceReal contains 'Google' then a.reg
         when a.sourceReal = 'CNIptv' then 'MET9**RKT'
         when a.sourceReal contains 'Facebook'
                or a.sourceReal contains 'Netflix'
                 or a.sourceReal contains 'Conversant' then 'MET9**KBL'
                        end AS rl1
  CALL {
    WITH a, rl1
    MATCH p=((from1:allNodes {nameReal: a.sourceReal})-[:PF*1..4]->(to1:allNodes {nameReal: rl1}))
    WITH from1, to1, a, p, [z in nodes(p) | z][1..size([z in nodes(p) | z])-1] AS listSize
    WHERE none(z in (listSize) WHERE z.nameReal contains 'Google')
    WITH distinct from1, to1, a, p,
      [z in nodes(p)| z] AS nodes,
      [z in relationships(p)| z] AS rels,
      [z in nodes(p)| z.nameReal] AS nodesName,
      size([z in nodes(p)| z])-1 AS Hops,
      apoc.coll.sum([z in relationships(p) | z.costLink]) AS Costs,
      [z in relationships(p) | z.cap] AS Capacity
    RETURN from1.nameReal AS Source, to1.nameReal AS Target, nodesName, Costs, Capacity, Hops, nodes, rels
    ORDER BY Hops, Costs asc
    LIMIT 1
  }
  WITH a, Source, Target, nodesName, Costs, Hops
  RETURN a, Source, Target, nodesName, Hops, Costs
  ORDER BY Source, Target
}
WITH a, nodesName, Costs, Hops
SET a.dest1=nodesName, a.cost1=Costs, a.hop1=Hops
----

== DL DEST2, FAST QUERY, DIRECTED () - FIRST - ME9 

Set 4158 properties, completed after 26439 ms.

[source,cypher]
----
CALL {
  MATCH (a:TRAFFIC_DOWN)
  WITH a
  CALL {
    WITH a
    MATCH p=((from1:allNodes {nameReal: last(a.dest1)})-[:PF*..5]->(to1:allNodes {nameReal: a.targetReal}))
    where from1.nameReal='MET9**KBL' or from1.nameReal='MET9**RKT'
    WITH from1, to1, a, p,
      [z in nodes(p) WHERE z:TIER1 | z.nameReal] AS TIER1,
      [z in nodes(p) WHERE z:TIER2 | z.nameReal] AS TIER2,
      [z in nodes(p) WHERE z:TIER3 | z.nameReal] AS TIER3,
      [z in nodes(p) WHERE z:MERKT | z.nameReal] AS MERKT,
      [z in nodes(p) WHERE z.nameReal=a.reg1 | z.nameReal] AS MUST,
      [z in tail(nodes(p)) WHERE z:CDN | z.nameReal] AS CDN,
      [z in nodes(p) WHERE z:TERA | z.nameReal] AS TERA,
      [z in tail(nodes(p)) WHERE z:OLT | z.nameReal] AS OLT,
      [z in nodes(p) | z][0..size([z in nodes(p) | z])-1] AS listSize,
      [z in nodes(p) WHERE z:TIER1 or z:TIER2 or z:TIER3| apoc.coll.removeAll(labels(z), ['ME','ME9'])[0]] AS TIERs,
      reverse(apoc.coll.sort([z in nodes(p) WHERE z:TIER1 or z:TIER2 or z:TIER3| apoc.coll.removeAll(labels(z), ['ME','ME9'])[0]])) AS SortedTIERs
    WHERE SortedTIERs = TIERs  
     AND none(z in (listSize) WHERE z.nameReal contains 'Google')
     AND size(TIER1)<=2
     AND size(TIER2)<=3
     AND size(TIER3)<=3
     AND size(MERKT)<=1
     AND size(CDN)=0
     AND size(TERA)=0
     AND size(OLT)=1
     AND size(MUST)=1
    WITH distinct from1, to1, a, p,
     [z in nodes(p)| z] AS nodes,
     [z in relationships(p)| z] AS rels,
     [z in nodes(p)| z.nameReal] AS nodesName,
     size([z in nodes(p)| z])-1 AS Hops,
     apoc.coll.sum([z in relationships(p) | z.costLink]) AS Costs,
     [z in relationships(p) | z.cap] AS Capacity
    RETURN from1.nameReal AS Source, to1.nameReal AS Target, nodesName, Costs, Capacity, Hops, nodes, rels
    ORDER BY Hops, Costs asc
    LIMIT 1
  }
  WITH a, Source, Target, nodesName, Costs, Hops
  RETURN a, Source, Target, nodesName, Hops, Costs
  ORDER BY Source, Target
}
WITH a, nodesName, Costs, Hops
SET a.dest2=nodesName, a.cost2=Costs, a.hop2=Hops
----

== DL DEST2, FAST QUERY, DIRECTED () - SECOND

Set 5049 properties, completed after 160264 ms.
from previous query result  4158
4158 + 5049 = 9,207 
THEN VALID 

[source,cypher]
----
CALL {
  MATCH (a:TRAFFIC_DOWN)
  WITH a
  CALL {
    WITH a
    MATCH p=((from1:allNodes {nameReal: last(a.dest1)})-[:PF]->(:BRAS)-[:PF]->(ME9)-[:PF*2..4]->(to1:allNodes {nameReal: a.targetReal}))
    where from1.nameReal='PE**RKT*HSI' or from1.nameReal='PE**KBL*HSI'
    WITH from1, to1, a, p,
      [z in nodes(p) WHERE z:TIER1 | z.nameReal] AS TIER1,
      [z in nodes(p) WHERE z:TIER2 | z.nameReal] AS TIER2,
      [z in nodes(p) WHERE z:TIER3 | z.nameReal] AS TIER3,
      [z in nodes(p) WHERE z:MERKT | z.nameReal] AS MERKT,
      [z in nodes(p) WHERE z.nameReal=a.reg1 | z.nameReal] AS MUST,
      [z in tail(nodes(p)) WHERE z:CDN | z.nameReal] AS CDN,
      [z in nodes(p) WHERE z:TERA | z.nameReal] AS TERA,
      [z in tail(nodes(p)) WHERE z:OLT | z.nameReal] AS OLT,
      [z in nodes(p) | z][0..size([z in nodes(p) | z])-1] AS listSize,
      [z in nodes(p) WHERE z:TIER1 or z:TIER2 or z:TIER3| apoc.coll.removeAll(labels(z), ['ME','ME9'])[0]] AS TIERs,
      reverse(apoc.coll.sort([z in nodes(p) WHERE z:TIER1 or z:TIER2 or z:TIER3| apoc.coll.removeAll(labels(z), ['ME','ME9'])[0]])) AS SortedTIERs
    WHERE SortedTIERs = TIERs  
     AND none(z in (listSize) WHERE z.nameReal contains 'Google')
     AND size(TIER1)<=2
     AND size(TIER2)<=3
     AND size(TIER3)<=3
     AND size(MERKT)<=1
     AND size(CDN)=0
     AND size(TERA)=0
     AND size(OLT)=1
     AND size(MUST)=1
    WITH distinct from1, to1, a, p,
     [z in nodes(p)| z] AS nodes,
     [z in relationships(p)| z] AS rels,
     [z in nodes(p)| z.nameReal] AS nodesName,
     size([z in nodes(p)| z])-1 AS Hops,
     apoc.coll.sum([z in relationships(p) | z.costLink]) AS Costs,
     [z in relationships(p) | z.cap] AS Capacity
    RETURN from1.nameReal AS Source, to1.nameReal AS Target, nodesName, Costs, Capacity, Hops, nodes, rels
    ORDER BY Hops, Costs asc
    LIMIT 1
  }
  WITH a, Source, Target, nodesName, Costs, Hops
  RETURN a, Source, Target, nodesName, Hops, Costs
  ORDER BY Source, Target
}
WITH a, nodesName, Costs, Hops
SET a.dest2=nodesName, a.cost2=Costs, a.hop2=Hops
----

== DL DEST, COST, HOP
[source,cypher]
----
MATCH (a:TRAFFIC_DOWN)
SET a.dest = (a.dest1)+tail(a.dest2), a.cost=(a.cost1)+(a.cost2), a.hop=(a.hop1)+(a.hop2)
----

END OF COMPLETE PATHFINDING QUERIES

== START COMPLETE TRAFFIC INJECTION

SET UL
[source,cypher]
----
MATCH (n)-[r:PF]-(m)
SET r.packet_loss_uplink = 0, r.uplink = 0,
  r.packet_loss_downlink = 0, r.downlink = 0,
  r.retail_uplink = 0, r.retail_downlink = 0,
  r.ebis_uplink = 0, r.ebis_downlink = 0,
  r.mobile_uplink = 0, r.mobile_downlink = 0,
  r.wholesale_uplink = 0, r.wholesale_downlink = 0, r.total_downlink = 0, r.nb_traffic_downlink = 0,
  r.occupancy_uplink = 0, r.occupancy_downlink = 0, r.total_uplink = 0, r.nb_traffic_uplink = 0,
  r.packet_success_uplink = 0, r.packet_success_downlink = 0
  , r.occupancy_downlink = 0, r.occupancy_uplink = 0
----

dispose uplink traffic
[source,cypher]
----
MATCH ()-[r:UPLINK]-() delete r
----

== UPLINK
[source,cypher]
----
MATCH (a:TRAFFIC_UP)
WITH a ORDER BY a.pri asc
//WITH a ORDER BY id(a) asc
CALL {
   WITH a
   MATCH (n1:allNodes {nameReal: a.dest[0]})
   MERGE (a)-[up:UPLINK]->(n1)
   SET up.real = a.traffic
   WITH a, up
   UNWIND a.dest AS x
      MATCH (b:allNodes {nameReal:x})
      WITH collect(b) AS x1, a, up
      CALL apoc.nodes.link(x1,'UP')
      MATCH (c)-[r:UP]->(d)
      MATCH (c)-[pf:PF]->(d)
      merge (c)-[u:UPLINK {real: a.traffic, uplink: a.traffic, cfu: a.cfu, ID: id(a), from1: a.sourceReal, to1: a.targetReal}]->(d)
      delete r
	 WITH a, collect(u) AS allUps1, collect(pf) AS allPFs, up
    WITH a, apoc.coll.insert(allUps1, 0, up) AS allUps,  allPFs
   RETURN allUps, allPFs
}
WITH a AS t, allUps, allPFs
FOREACH (i in range(0, size(allPFs) - 1) | foreach( r in [allPFs[i]] | foreach( prev in [allUps[i]] | foreach( current in [allUps[i+1]] |
SET current.real = CASE WHEN r.cap - r.uplink - prev.real > 0 THEN prev.real ELSE r.cap - r.uplink END,
 r.total_uplink = r.total_uplink + prev.real, r.nb_traffic_uplink = r.nb_traffic_uplink+1,
 r.retail_uplink =
    CASE WHEN current.cfu CONTAINS 'RETAIL' THEN r.retail_uplink + toFloat(current.real) ELSE r.retail_uplink END,
  r.ebis_uplink =
    CASE WHEN current.cfu CONTAINS 'EBIS' THEN r.ebis_uplink+toFloat(current.real) ELSE r.ebis_uplink END,
  r.mobile_uplink =
    CASE WHEN current.cfu CONTAINS 'MOBILE' THEN r.mobile_uplink+toFloat(current.real) ELSE r.mobile_uplink END,
  r.wholesale_uplink =
    CASE WHEN current.cfu CONTAINS 'WHOLESALE' THEN r.wholesale_uplink+toFloat(current.real) ELSE r.wholesale_uplink END
FOREACH (i in range(0, size(allPFs) - 1) | foreach( r in [allPFs[i]] |
  SET r.uplink=toFloat(r.retail_uplink+r.ebis_uplink+r.mobile_uplink+r.wholesale_uplink)
))
))))
FOREACH (i in range(0, size(allPFs) - 1) | foreach( r in [allPFs[i]] |
 SET r.packet_success_uplink=toFloat(r.retail_uplink+r.ebis_uplink+r.mobile_uplink+r.wholesale_uplink),
 r.packet_loss_uplink = apoc.coll.max([0, r.total_uplink - r.uplink]),
 r.occupancy_uplink = round(toFloat(r.uplink/r.cap),3)))
----

== Downlink

dispose traffic downlink
[source,cypher]
----
MATCH ()-[r:DOWNLINK]-() delete r;
----

[source,cypher]
----
MATCH (a:TRAFFIC_DOWN)
WITH a ORDER BY a.pri asc
//WITH a ORDER BY id(a) asc
CALL {
   WITH a
   MATCH (n1:allNodes {nameReal: a.dest[0]})
   MERGE (a)-[up:DOWNLINK]->(n1)
   SET up.real = a.traffic
   WITH a, up
   UNWIND a.dest AS x
      MATCH (b:allNodes {nameReal:x})
      WITH collect(b) AS x1, a, up
      CALL apoc.nodes.link(x1,'DOWN')
      MATCH (c)-[r:DOWN]->(d)
      MATCH (c)-[pf:PF]->(d)
      merge (c)-[u:DOWNLINK {real: a.traffic, downlink: a.traffic, cfu: a.cfu, ID: id(a), from1: a.sourceReal, to1: a.targetReal}]->(d)
      delete r
	 WITH a, collect(u) AS allUps1, collect(pf) AS allPFs, up
    WITH a, apoc.coll.insert(allUps1, 0, up) AS allUps,  allPFs
   RETURN allUps, allPFs
}
WITH a AS t, allUps, allPFs
FOREACH (i in range(0, size(allPFs) - 1) | foreach( r in [allPFs[i]] | foreach( prev in [allUps[i]] | foreach( current in [allUps[i+1]] |
SET current.real = CASE WHEN r.cap - r.downlink - prev.real > 0 THEN prev.real ELSE r.cap - r.downlink END,
 r.total_downlink = r.total_downlink + prev.real, r.nb_traffic_downlink = r.nb_traffic_downlink+1,
 r.retail_downlink =
    CASE WHEN current.cfu CONTAINS 'RETAIL' THEN r.retail_downlink + toFloat(current.real) ELSE r.retail_downlink END,
  r.ebis_downlink =
    CASE WHEN current.cfu CONTAINS 'EBIS' THEN r.ebis_downlink+toFloat(current.real) ELSE r.ebis_downlink END,
  r.mobile_downlink =
    CASE WHEN current.cfu CONTAINS 'MOBILE' THEN r.mobile_downlink+toFloat(current.real) ELSE r.mobile_downlink END,
  r.wholesale_downlink =
    CASE WHEN current.cfu CONTAINS 'WHOLESALE' THEN r.wholesale_downlink+toFloat(current.real) ELSE r.wholesale_downlink END
FOREACH (i in range(0, size(allPFs) - 1) | foreach( r in [allPFs[i]] |
  SET r.downlink=toFloat(r.retail_downlink+r.ebis_downlink+r.mobile_downlink+r.wholesale_downlink)
))
))))
FOREACH (i in range(0, size(allPFs) - 1) | foreach( r in [allPFs[i]] |
 SET r.packet_success_downlink=toFloat(r.retail_downlink+r.ebis_downlink+r.mobile_downlink+r.wholesale_downlink),
 r.packet_loss_downlink = apoc.coll.max([0, r.total_downlink - r.downlink]),
 r.occupancy_downlink = round(toFloat(r.downlink/r.cap),3)))
----

== SET PF
[source,cypher]
----
MATCH ()-[r:PF]-()
SET 
r.packet_loss_uplink=round(r.packet_loss_uplink, 4)
, r.packet_loss_downlink=round(r.packet_loss_downlink, 4)
, r.ebis_uplink=round(r.ebis_uplink, 4)
, r.ebis_downlink=round(r.ebis_downlink, 4)
, r.retail_uplink=round(r.retail_uplink, 4)
, r.retail_downlink=round(r.retail_downlink, 4)
, r.mobile_uplink=round(r.mobile_uplink, 4)
, r.mobile_downlink=round(r.mobile_downlink, 4)
, r.wholesale_uplink=round(r.wholesale_uplink, 4)
, r.wholesale_downlink=round(r.wholesale_downlink, 4)
, r.uplink=round(r.uplink, 4)
, r.downlink=round(r.downlink, 4)
, r.packet_success_uplink=round(r.packet_success_uplink, 4)
, r.packet_success_downlink=round(r.packet_success_downlink, 4)
----

== RESET LINKS VALUE
[source,cypher]
----
match p= ()-[r:LINKS]->()
set
r.uplink=round(toFloat(0),5),
r.downlink=round(toFloat(0),5),
r.occupancy_uplink=round(toFloat(0),5),
r.occupancy_downlink=round(toFloat(0),5),
r.mobile_uplink=round(toFloat(0),5),
r.mobile_downlink=round(toFloat(0),5),
r.retail_uplink=round(toFloat(0),5),
r.retail_downlink=round(toFloat(0),5),
r.ebis_uplink=round(toFloat(0),5),
r.ebis_downlink=round(toFloat(0),5),
r.wholesale_uplink=round(toFloat(0),5),
r.wholesale_downlink=round(toFloat(0),5),
r.packet_loss_uplink=round(toFloat(0),5),
r.packet_loss_downlink=round(toFloat(0),5),
r.packet_success_uplink=round(toFloat(0),5),
r.packet_success_downlink=round(toFloat(0),5)
----

[source,cypher]
----
MATCH (a)-[r:PF]->(b),(a)-[rr:LINKS]-(b)
WHERE r.downlink > 0
    SET
    rr.downlink=r.downlink
    , rr.packet_loss_downlink=r.packet_loss_downlink
    , rr.packet_success_downlink=r.packet_success_downlink
    , rr.ebis_downlink=r.ebis_downlink
    , rr.retail_downlink=r.retail_downlink
    , rr.mobile_downlink=r.mobile_downlink
    , rr.wholesale_downlink=r.wholesale_downlink
    , rr.occupancy_downlink=r.occupancy_downlink
----

[source,cypher]
----
MATCH (a)-[r:PF]->(b),(a)-[rr:LINKS]-(b)
WHERE r.uplink > 0
    SET
    rr.uplink=r.uplink
    , rr.packet_loss_uplink=r.packet_loss_uplink
    , rr.packet_success_uplink=r.packet_success_uplink
    , rr.ebis_uplink=r.ebis_uplink
    , rr.retail_uplink=r.retail_uplink
    , rr.mobile_uplink=r.mobile_uplink
    , rr.wholesale_uplink=r.wholesale_uplink
    , rr.occupancy_uplink=r.occupancy_uplink
----
    
END OF COMPLETE TRAFFIC INJECTION

== START PROCESS PATHFINDING (WITH OCCUPANCY)

REMOVE TIERS
[source,cypher]
----
MATCH (n:ME)
REMOVE n:TIER1, n:TIER2, n:TIER3
----

Create TIER1 Label
[source,cypher]
----
MATCH (a:ME)-[r:PF]-(b:BRAS)
SET a:TIER1
----

CREATE TIER2 LABEL
[source,cypher]
----
MATCH (a:ME)-[:PF]-(:TIER1)
WHERE not a:TIER1
SET a:TIER2
----

CREATE TIER3 LABEL
[source,cypher]
----
MATCH p=(n:ME)
WHERE none(a in nodes(p) WHERE a:TIER1 or a:TIER2)
SET n:TIER3
----

== UL DEST1, FAST QUERY, DIRECTED ()
[source,cypher]
----
CALL {
  MATCH (a:TRAFFIC_UP)
  WITH a
  CALL {
    WITH a
    MATCH p=((from1:OLT {nameReal: a.sourceReal})-[:PF*2..4]->(:TIER1)-[:PF]->(:BRAS)-[:PF]->(to1:PE))
    WITH from1, to1, a, p
    , [z in nodes(p) WHERE z:TIER1 | z.nameReal] AS TIER1
    , [z in nodes(p) WHERE z:TIER2 | z.nameReal] AS TIER2
    , [z in nodes(p) WHERE z:TIER3 | z.nameReal] AS TIER3
    , [z in nodes(p) WHERE z:CDN | z.nameReal] AS CDN
    , [z in nodes(p) WHERE z:TERA | z.nameReal] AS TERA
    , [z in tail(nodes(p)) WHERE z:OLT | z.nameReal] AS OLT
    , [z in nodes(p) | z][0..size([z in nodes(p) | z])-1] AS listSize
    WHERE 
     none(z in (listSize) WHERE z.nameReal contains 'Google')
     AND size(TIER1)=1
     AND size(CDN)=0
     AND size(TERA)=0
     AND size(OLT)=0
    WITH DISTINCT from1, to1, a, p,
    [z in nodes(p)| z] AS nodes,
    [z in relationships(p)| z] AS rels,
    [z in nodes(p)| z.nameReal] AS nodesName,
    size([z in nodes(p)| z])-1 AS Hops,
    apoc.coll.sum([z in relationships(p) | z.costLink]) AS Costs,
    [z in relationships(p) | z.cap] AS Capacity,
    apoc.coll.sum([z in relationships(p) | z.occupancy_uplink]) AS occUp
    RETURN from1.nameReal AS Source, to1.nameReal AS Target, nodesName, Costs, Capacity, Hops, nodes, rels, occUp
    ORDER BY Hops, Costs, occUp asc
    LIMIT 1
  }
  WITH a, Source, Target, nodesName, Costs, Hops, occUp
  RETURN a, Source, Target, nodesName, Hops, Costs, occUp
  ORDER BY Source, Target
}
WITH a, nodesName, Costs, Hops
SET a.dest1=nodesName, a.cost1=Costs, a.hop1=Hops
----

== UL DEST2, FAST QUERY, DIRECTED ()
[source,cypher]
----
CALL {
  MATCH (a:TRAFFIC_UP)
  WITH a
  CALL {
    WITH a
    MATCH p=((from1:allNodes {nameReal: last(a.dest1)})-[:PF*1..4]->(to1:allNodes {nameReal: a.targetReal}))
    WITH from1, to1, a, p, 
      [z in nodes(p) | z][0..size([z in nodes(p) | z])-1] AS listSize
    WHERE none(z in (listSize) WHERE z:CGW or z:CDN)
    WITH distinct from1, to1, a, p,
      [z in nodes(p)| z] AS nodes,
      [z in relationships(p)| z] AS rels,
      [z in nodes(p)| z.nameReal] AS nodesName,
      size([z in nodes(p)| z])-1 AS Hops,
      apoc.coll.sum([z in relationships(p) | z.costLink]) AS Costs,
      [z in relationships(p) | z.cap] AS Capacity,
      apoc.coll.sum([z in relationships(p) | z.occupancy_uplink]) AS occUp
    RETURN from1.nameReal AS Source, to1.nameReal AS Target, nodesName, Costs, Capacity, Hops, nodes, rels, occUp
    ORDER BY Hops, Costs, occUp asc
    LIMIT 1
  }
  WITH a, Source, Target, nodesName, Costs, Hops, occUp
  RETURN a, Source, Target, nodesName, Hops, Costs, occUp
  ORDER BY Source, Target
}
WITH a, nodesName, Costs, Hops
SET a.dest2=nodesName, a.cost2=Costs, a.hop2=Hops
----

== UL PREPARE DL REG
[source,cypher]
----
MATCH (a:TRAFFIC_UP)
SET a.dest = (a.dest1)+tail(a.dest2), a.cost=(a.cost1)+(a.cost2), a.hop=(a.hop1)+(a.hop2),
  a.reg=last(a.dest1),
  a.reg1=case when last(a.dest1) contains 'RKT' then 'MET9**RKT'
         when last(a.dest1) contains 'KBL' then 'MET9**KBL' end
WITH a
MATCH (m:TRAFFIC_DOWN {targetReal: a.sourceReal})
WITH m,a
SET m.reg=a.reg, m.reg1=a.reg1
----

== DL DEST1, FAST QUERY, DIRECTED () FIRST
[source,cypher]
----
CALL {
  MATCH (a:TRAFFIC_DOWN)
  WITH a
  , case
         when a.sourceReal contains 'CGW' then a.reg
         when a.sourceReal contains 'Google' then a.reg
         when a.sourceReal = 'CNIptv' then 'MET9**RKT'
         when a.sourceReal contains 'Facebook'
                or a.sourceReal contains 'Netflix'
                 or a.sourceReal contains 'Conversant' then 'MET9**KBL'
                        end AS rl1
  CALL {
    WITH a, rl1
    MATCH p=((from1:allNodes {nameReal: a.sourceReal})-[:PF*1..4]->(to1:allNodes {nameReal: rl1}))
    WITH from1, to1, a, p, [z in nodes(p) | z][1..size([z in nodes(p) | z])-1] AS listSize
    WHERE none(z in (listSize) WHERE z.nameReal contains 'Google')
    WITH distinct from1, to1, a, p,
      [z in nodes(p)| z] AS nodes,
      [z in relationships(p)| z] AS rels,
      [z in nodes(p)| z.nameReal] AS nodesName,
      size([z in nodes(p)| z])-1 AS Hops,
      apoc.coll.sum([z in relationships(p) | z.costLink]) AS Costs,
      [z in relationships(p) | z.cap] AS Capacity,
      apoc.coll.sum([z in relationships(p) | z.occupancy_downlink]) AS occDown
    RETURN from1.nameReal AS Source, to1.nameReal AS Target, nodesName, Costs, Capacity, Hops, nodes, rels, occDown
    ORDER BY Hops, Costs, occDown asc
    LIMIT 1
  }
  WITH a, Source, Target, nodesName, Costs, Hops
  RETURN a, Source, Target, nodesName, Hops, Costs
  ORDER BY Source, Target
}
WITH a, nodesName, Costs, Hops
SET a.dest1=nodesName, a.cost1=Costs, a.hop1=Hops
----

== DL DEST2, FAST QUERY, DIRECTED () - SECOND - ME9
[source,cypher]
----
CALL {
  MATCH (a:TRAFFIC_DOWN)
  WITH a
  CALL {
    WITH a
    MATCH p=((from1:allNodes {nameReal: last(a.dest1)})-[:PF*..5]->(to1:allNodes {nameReal: a.targetReal}))
    where from1.nameReal='MET9**KBL' or from1.nameReal='MET9**RKT'
    WITH from1, to1, a, p,
      [z in nodes(p) WHERE z:TIER1 | z.nameReal] AS TIER1,
      [z in nodes(p) WHERE z:TIER2 | z.nameReal] AS TIER2,
      [z in nodes(p) WHERE z:TIER3 | z.nameReal] AS TIER3,
      [z in nodes(p) WHERE z:MERKT | z.nameReal] AS MERKT,
      [z in nodes(p) WHERE z.nameReal=a.reg1 | z.nameReal] AS MUST,
      [z in tail(nodes(p)) WHERE z:CDN | z.nameReal] AS CDN,
      [z in nodes(p) WHERE z:TERA | z.nameReal] AS TERA,
      [z in tail(nodes(p)) WHERE z:OLT | z.nameReal] AS OLT,
      [z in nodes(p) | z][0..size([z in nodes(p) | z])-1] AS listSize,
      [z in nodes(p) WHERE z:TIER1 or z:TIER2 or z:TIER3| apoc.coll.removeAll(labels(z), ['ME','ME9'])[0]] AS TIERs,
      reverse(apoc.coll.sort([z in nodes(p) WHERE z:TIER1 or z:TIER2 or z:TIER3| apoc.coll.removeAll(labels(z), ['ME','ME9'])[0]])) AS SortedTIERs
    WHERE SortedTIERs = TIERs  
     AND none(z in (listSize) WHERE z.nameReal contains 'Google')
     AND size(TIER1)<=2
     AND size(TIER2)<=3
     AND size(TIER3)<=3
     AND size(MERKT)<=1
     AND size(CDN)=0
     AND size(TERA)=0
     AND size(OLT)=1
     AND size(MUST)=1
    WITH distinct from1, to1, a, p,
     [z in nodes(p)| z] AS nodes,
     [z in relationships(p)| z] AS rels,
     [z in nodes(p)| z.nameReal] AS nodesName,
     size([z in nodes(p)| z])-1 AS Hops,
     apoc.coll.sum([z in relationships(p) | z.costLink]) AS Costs,
     [z in relationships(p) | z.cap] AS Capacity,
     apoc.coll.sum([z in relationships(p) | z.occupancy_downlink]) AS occDown
    RETURN from1.nameReal AS Source, to1.nameReal AS Target, nodesName, Costs, Capacity, Hops, nodes, rels, occDown
    ORDER BY Hops, Costs, occDown asc
    LIMIT 1
  }
  WITH a, Source, Target, nodesName, Costs, Hops
  RETURN a, Source, Target, nodesName, Hops, Costs
  ORDER BY Source, Target
}
WITH a, nodesName, Costs, Hops
SET a.dest2=nodesName, a.cost2=Costs, a.hop2=Hops
----

== DL DEST2, FAST QUERY, DIRECTED () - THIRD
[source,cypher]
----
CALL {
  MATCH (a:TRAFFIC_DOWN)
  WITH a
  CALL {
    WITH a
    MATCH p=((from1:allNodes {nameReal: last(a.dest1)})-[:PF]->(:BRAS)-[:PF]->(ME9)-[:PF*2..4]->(to1:allNodes {nameReal: a.targetReal}))
    where from1.nameReal='PE**RKT*HSI' or from1.nameReal='PE**KBL*HSI'
    WITH from1, to1, a, p,
      [z in nodes(p) WHERE z:TIER1 | z.nameReal] AS TIER1,
      [z in nodes(p) WHERE z:TIER2 | z.nameReal] AS TIER2,
      [z in nodes(p) WHERE z:TIER3 | z.nameReal] AS TIER3,
      [z in nodes(p) WHERE z:MERKT | z.nameReal] AS MERKT,
      [z in nodes(p) WHERE z.nameReal=a.reg1 | z.nameReal] AS MUST,
      [z in tail(nodes(p)) WHERE z:CDN | z.nameReal] AS CDN,
      [z in nodes(p) WHERE z:TERA | z.nameReal] AS TERA,
      [z in tail(nodes(p)) WHERE z:OLT | z.nameReal] AS OLT,
      [z in nodes(p) | z][0..size([z in nodes(p) | z])-1] AS listSize,
      [z in nodes(p) WHERE z:TIER1 or z:TIER2 or z:TIER3| apoc.coll.removeAll(labels(z), ['ME','ME9'])[0]] AS TIERs,
      reverse(apoc.coll.sort([z in nodes(p) WHERE z:TIER1 or z:TIER2 or z:TIER3| apoc.coll.removeAll(labels(z), ['ME','ME9'])[0]])) AS SortedTIERs
    WHERE SortedTIERs = TIERs  
     AND none(z in (listSize) WHERE z.nameReal contains 'Google')
     AND size(TIER1)<=2
     AND size(TIER2)<=3
     AND size(TIER3)<=3
     AND size(MERKT)<=1
     AND size(CDN)=0
     AND size(TERA)=0
     AND size(OLT)=1
     AND size(MUST)=1
    WITH distinct from1, to1, a, p,
     [z in nodes(p)| z] AS nodes,
     [z in relationships(p)| z] AS rels,
     [z in nodes(p)| z.nameReal] AS nodesName,
     size([z in nodes(p)| z])-1 AS Hops,
     apoc.coll.sum([z in relationships(p) | z.costLink]) AS Costs,
     [z in relationships(p) | z.cap] AS Capacity,
     apoc.coll.sum([z in relationships(p) | z.occupancy_downlink]) AS occDown
    RETURN from1.nameReal AS Source, to1.nameReal AS Target, nodesName, Costs, Capacity, Hops, nodes, rels, occDown
    ORDER BY Hops, Costs, occDown asc
    LIMIT 1
  }
  WITH a, Source, Target, nodesName, Costs, Hops
  RETURN a, Source, Target, nodesName, Hops, Costs
  ORDER BY Source, Target
}
WITH a, nodesName, Costs, Hops
SET a.dest2=nodesName, a.cost2=Costs, a.hop2=Hops
----

== DL DEST, COST, HOP
[source,cypher]
----
MATCH (a:TRAFFIC_DOWN)
SET a.dest = (a.dest1)+tail(a.dest2), a.cost=(a.cost1)+(a.cost2), a.hop=(a.hop1)+(a.hop2)
----

END OF COMPLETE PATHFINDING QUERIES

