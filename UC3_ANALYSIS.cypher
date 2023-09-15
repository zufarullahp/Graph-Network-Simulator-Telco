// search for critical LINKS (n >=100%)
match p= ()-[r:LINKS]->()
where r.occupancy_downlink=1 or r.occupancy_uplink=1
return p;

// search for critical LINKS (75% < n < 100%)
match p= ()-[r:LINKS]->()
where 0.75 < r.occupancy_downlink < 1 or 0.75 < r.occupancy_uplink < 1
return p;

// search for critical LINKS (50% < n < 75%)
match p= ()-[r:LINKS]->()
where 0.5 < r.occupancy_downlink < 0.75 or 0.5 < r.occupancy_uplink < 0.75
return p;

// search for critical LINKS (n < 50%)
match p= ()-[r:LINKS]->()
where r.occupancy_downlink < 0.5 or r.occupancy_uplink < 0.5
return p;