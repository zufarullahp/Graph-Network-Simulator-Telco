// COMPLETE DATA INGESTION

// OLT-ME
load csv with headers from 'file:///topologyTlkm.csv' as row
with row where row.ruas = 'ME-AKSES'
with case when row.cost_link is not null then row.cost_link else 0 end as cl, row
merge (a:OLT {nameReal:row.from1})
merge (b:ME {nameReal:row.to1})
merge (a)-[r:LINK {ruas:row.ruas, port:row.port, cap:row.kap, costLink:cl}]->(b)
;

// ME-ME
load csv with headers from 'file:///topologyTlkm.csv' as row
with row where row.ruas = 'ME-ME'
with case when row.cost_link is not null then row.cost_link else 0 end as cl, row
merge (a:ME {nameReal:row.from1})
merge (b:ME {nameReal:row.to1})
merge (a)-[r:LINK {ruas:row.ruas, port:row.port, cap:row.kap, costLink:cl}]->(b)
;

// ME-BRAS
load csv with headers from 'file:///topologyTlkm.csv' as row
with row where row.ruas = 'ME-BRAS'
with case when row.cost_link is not null then row.cost_link else 0 end as cl, row
merge (a:BRAS {nameReal:row.from1})
merge (b:ME {nameReal:row.to1})
merge (b)-[r:LINK {ruas:row.ruas, port:row.port, cap:row.kap, costLink:cl}]->(a)
;

// BRAS-PE
load csv with headers from 'file:///topologyTlkm.csv' as row
with row where row.ruas = 'BRAS-PE'
with case when row.cost_link is not null then row.cost_link else 0 end as cl, row
merge (a:BRAS {nameReal:row.from1})
merge (b:PE {nameReal:row.to1})
merge (a)-[r:LINK {ruas:row.ruas, port:row.port, cap:row.kap, costLink:cl}]->(b)
;

// PE-TERA
load csv with headers from 'file:///topologyTlkm.csv' as row
with row where row.ruas = 'PE-TERA'
with case when row.cost_link is not null then row.cost_link else 0 end as cl, row
merge (a:PE {nameReal:row.from1})
merge (b:TERA {nameReal:row.to1})
merge (a)-[r:LINK {ruas:row.ruas, port:row.port, cap:row.kap, costLink:cl}]->(b)
;

// TERA-TERA
load csv with headers from 'file:///topologyTlkm.csv' as row
with row where row.ruas = 'TERA-TERA'
with case when row.cost_link is not null then row.cost_link else 0 end as cl, row
merge (a:TERA {nameReal:row.from1})
merge (b:TERA {nameReal:row.to1})
merge (a)-[r:LINK {ruas:row.ruas, port:row.port, cap:row.kap, costLink:cl}]->(b)
;

// TERA-CGW
load csv with headers from 'file:///topologyTlkm.csv' as row
with row where row.ruas = 'TERA-CGW'
with case when row.cost_link is not null then row.cost_link else 0 end as cl, row
merge (a:TERA {nameReal:row.from1})
merge (b:CGW {nameReal:row.to1})
merge (a)-[r:LINK {ruas:row.ruas, port:row.port, cap:row.kap, costLink:cl}]->(b)
;

// PE-CDN
load csv with headers from 'file:///topologyTlkm.csv' as row
with row where row.ruas = 'PE-CDN'
with case when row.cost_link is not null then row.cost_link else 0 end as cl, row
merge (a:PE {nameReal:row.from1})
merge (b:CDN {nameReal:row.to1})
merge (a)-[r:LINK {ruas:row.ruas, port:row.port, cap:row.kap, costLink:cl}]->(b)
;

// PE-DATIN #1
load csv with headers from 'file:///topologyTlkm.csv' as row
with row where row.ruas = 'PE-DATIN' and row.from1 starts with 'MET'
with case when row.cost_link is not null then row.cost_link else 0 end as cl, row
merge (a:ME {nameReal:row.from1})
merge (b:PE {nameReal:row.to1})
merge (a)-[r:LINK {ruas:row.ruas, port:row.port, cap:row.kap, costLink:cl}]->(b)
;

// PE-DATIN #2
load csv with headers from 'file:///topologyTlkm.csv' as row
with row where row.ruas = 'PE-DATIN' and row.from1 starts with 'PE'
with case when row.cost_link is not null then row.cost_link else 0 end as cl, row
merge (a:PE {nameReal:row.from1})
merge (b:ME {nameReal:row.to1})
merge (a)-[r:LINK {ruas:row.ruas, port:row.port, cap:row.kap, costLink:cl}]->(b)
;

// PE-DATIN #3
load csv with headers from 'file:///topologyTlkm.csv' as row
with row where row.ruas = 'PE-DATIN' and row.from1 starts with 'P*'
with case when row.cost_link is not null then row.cost_link else 0 end as cl, row
merge (a:TERA {nameReal:row.from1})
merge (b:ME {nameReal:row.to1})
merge (a)-[r:LINK {ruas:row.ruas, port:row.port, cap:row.kap, costLink:cl}]->(b)
;

// PE-DATIN #4
load csv with headers from 'file:///topologyTlkm.csv' as row
with row where row.ruas = 'PE-DATIN' and row.from1 starts with 'PE'
with case when row.cost_link is not null then row.cost_link else 0 end as cl, row
merge (a:PE {nameReal:row.from1})
merge (b:TERA {nameReal:row.to1})
merge (a)-[r:LINK {ruas:row.ruas, port:row.port, cap:row.kap, costLink:cl}]->(b)
;

/////////////////////////////////////////////////////////////////////////////////////////////////////
// ERROR CHECK
match (a:ME) 
where not a.nameReal starts with 'MET'
detach delete a
;

match (a:BRAS) 
where not a.nameReal starts with 'BR'
detach delete a
;

match (a:TERA) 
where not a.nameReal starts with 'P*' or a.nameReal starts with 'T*'
detach delete a
;

// MERGE LINK WITH DISTINCT PORTS
load csv with headers from 'file:///topologyTlkm.csv' as row
with row
match (a {nameReal:row.from1}),(b {nameReal:row.to1})
with distinct a, b, row.port as port, row.kap as cap,
case when row.cost_link is null then 0 else row.cost_link end as cost
with sum(toInteger(cap)) as zum, min(cost) as cst, count(port) as ports, a, b
//return a.nameReal, b.nameReal, zum, cst, ports
 merge (a)-[:LINKS {cap:zum, costLink:cst, portSum:ports}]->(b)
;

// SET INITIAL LINK PROPERTIES
match p= ()-[r:LINKS]->()
set
r.uplink=round(toFloat(0),10),
r.downlink=round(toFloat(0),10),
r.occupancy_uplink=round(toFloat(0),10),
r.occupancy_downlink=round(toFloat(0),10),
r.mobile_uplink=round(toFloat(0),10),
r.mobile_downlink=round(toFloat(0),10),
r.retail_uplink=round(toFloat(0),10),
r.retail_downlink=round(toFloat(0),10),
r.ebis_uplink=round(toFloat(0),10),
r.ebis_downlink=round(toFloat(0),10),
r.wholesale_uplink=round(toFloat(0),10),
r.wholesale_downlink=round(toFloat(0),10),
r.packet_loss_uplink=round(toFloat(0),10),
r.packet_loss_downlink=round(toFloat(0),10),
r.packet_success_uplink=round(toFloat(0),10),
r.packet_success_downlink=round(toFloat(0),10)
;

// CREATE GLOBAL LABEL
match (n) set n:allNodes
;

// UPLINK TRAFFIC
call {
load csv with headers from 'file:///trafficUpTlkm.csv' as row
with row 

    return row.cfu as cfu, row.to1 as target, sum(toFloat(row.bw_up)) as traffic, row.olt1 as source 
}
with target, source, traffic, cfu
merge (:TRAFFIC_UP {sourceReal: source, targetReal: target, traffic: traffic, cfu: cfu})
;

// DOWNLINK TRAFFIC
call {
load csv with headers from 'file:///trafficDownTlkm.csv' as row
with row 
    return row.cfu as cfu, row.olt1 as target, sum(toFloat(row.bw_down)) as traffic, row.from1 as source 
}
with target, source, traffic, cfu
merge (:TRAFFIC_DOWN {sourceReal: source, targetReal: target, traffic: traffic, cfu: cfu})
;

// CREATE TRAFFIC PRIORITIES
match (a:TRAFFIC_UP)
where a.cfu = 'MOBILE'
set a.pri = 1;

match (a:TRAFFIC_UP)
where a.cfu = 'RETAIL'
set a.pri = 2;

match (a:TRAFFIC_UP)
where a.cfu = 'EBIS'
set a.pri = 3;

match (a:TRAFFIC_UP)
where a.cfu = 'WHOLESALE'
set a.pri = 4;

match (a:TRAFFIC_DOWN)
where a.cfu = 'MOBILE'
set a.pri = 1;

match (a:TRAFFIC_DOWN)
where a.cfu = 'RETAIL'
set a.pri = 2;

match (a:TRAFFIC_DOWN)
where a.cfu = 'EBIS'
set a.pri = 3;

match (a:TRAFFIC_DOWN)
where a.cfu = 'WHOLESALE'
set a.pri = 4;