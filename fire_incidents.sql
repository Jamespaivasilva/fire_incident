
create database incidents;

use incidents;

# Create table tmp to receive data from csv file
CREATE TABLE IF NOT EXISTS incidents.fire_incidents_tmp (
  `incident_number` text DEFAULT NULL,
  `exposure_number` text,
  `id` text,
  `address` text,
  `incident_date` DATETIME NULL DEFAULT NULL,
  `call_number` text,
  `alarm_dttm` DATETIME NULL DEFAULT NULL,
  `arrival_dttm` DATETIME NULL DEFAULT NULL,
  `close_dttm` DATETIME NULL DEFAULT NULL,
  `city` text,
  `zipcode` text,
  `battalion` text,
  `station_area` text,
  `box` text,
  `suppression_units` int DEFAULT NULL,
  `suppression_personnel` int DEFAULT NULL,
  `ems_units` int DEFAULT NULL,
  `ems_personnel` int DEFAULT NULL,
  `other_units` int DEFAULT NULL,
  `other_personnel` int DEFAULT NULL,
  `first_unit_on_scene` text,
  `estimated_property_loss` int DEFAULT NULL,
  `estimated_contents_loss` int DEFAULT NULL,
  `fire_fatalities` int DEFAULT NULL,
  `fire_injuries` int DEFAULT NULL,
  `civilian_fatalities` int DEFAULT NULL,
  `civilian_injuries` int DEFAULT NULL,
  `number_of_alarms` int DEFAULT NULL,
  `primary_situation` text,
  `mutual_aid` text,
  `action_taken_primary` text,
  `action_taken_secondary` text,
  `action_taken_other` text,
  `detector_alerted_occupants` text,
  `property_use` text,
  `area_of_fire_origin` text,
  `ignition_cause` text,
  `ignition_factor_primary` text,
  `ignition_factor_secondary` text,
  `heat_source` text,
  `item_first_ignited` text,
  `human_factors_associated_with_ignition` text,
  `structure_type` text,
  `structure_status` text,
  `floor_of_fire_origin` int DEFAULT NULL,
  `fire_spread` text,
  `no_flame_spread` text,
  `number_of_floors_with_minimum_damage` int DEFAULT NULL,
  `number_of_floors_with_significant_damage` int DEFAULT NULL,
  `number_of_floors_with_heavy_damage` int DEFAULT NULL,
  `number_of_floors_with_extreme_damage` int DEFAULT NULL,
  `detectors_present` text,
  `detector_type` text,
  `detector_operation` text,
  `detector_effectiveness` text,
  `detector_failure_reason` text,
  `automatic_extinguishing_system_present` text,
  `automatic_extinguishing_sytem_type` text,
  `automatic_extinguishing_sytem_perfomance` text,
  `automatic_extinguishing_sytem_failure_reason` text,
  `number_of_sprinkler_heads_operating` int DEFAULT NULL,
  `supervisor_district` text,
  `neighborhood_district` text,
  `point` text,
  `data_as_of` DATETIME NULL DEFAULT NULL,
  `data_loaded_at` DATETIME NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;







CREATE TABLE IF NOT EXISTS incidents.dim_battalion(
battalion_id int AUTO_INCREMENT PRIMARY KEY,
battalion VARCHAR(20)
)

# Inserting only news values into battalion dimension
insert into incidents.dim_battalion(battalion)
select distinct t1.battalion 
from incidents.fire_incidents_tmp as t1
left join incidents.dim_battalion as t2
on t1.battalion = t2.battalion
where t2.battalion is null 
and t1.battalion is not null


select distinct battalion
from `incidents`.`fire_incidents_tmp`


# validating neighborhood_district on tmp
select distinct neighborhood_district
from `incidents`.`fire_incidents_tmp`

# Creating dim_district dimension table
CREATE TABLE IF NOT EXISTS incidents.dim_district(
id_district int AUTO_INCREMENT PRIMARY KEY,
neighborhood_district VARCHAR(45)
)


# Inserting only news values into dimension district
INSERT INTO incidents.dim_district (neighborhood_district)
select distinct t1.neighborhood_district
from `incidents`.`fire_incidents_tmp` as t1
left join incidents.dim_district as t2
on t1.neighborhood_district = t2.neighborhood_district
where t2.neighborhood_district is null 
and t1.neighborhood_district is not null;


# Creating dim_time_period dimension table
CREATE TABLE IF NOT EXISTS incidents.dim_time_period (time_period_id int AUTO_INCREMENT PRIMARY KEY,
alarm_time time, alarm_hour int, alarm_min int, alarm_sec int);


# Inserting only news values into time_period
insert into incidents.dim_time_period(alarm_time, alarm_hour, alarm_min, alarm_sec)
select distinct time(alarm_dttm) as alarm_time,
hour(alarm_dttm) as alarm_hour,
minute(alarm_dttm) as alarm_min,
second(alarm_dttm) as alarm_sec
FROM incidents.fire_incidents_tmp as t1
left join incidents.dim_time_period as t2
on time(t1.alarm_dttm) = t2.alarm_time
where t2.alarm_time is null
and time(t1.alarm_dttm) is not null;

s

# Creating Fact Table 
CREATE TABLE IF NOT EXISTS incidents.fire_incidents_fact (
  `incident_number` int PRIMARY KEY ,
  `exposure_number` text,
   time_period_id int,
   battalion_id int,
   id_district int,
  `id` text,
  `address` text,
  `incident_date` DATETIME NULL DEFAULT NULL,
  `call_number` text,
  `alarm_dttm` DATETIME NULL DEFAULT NULL,
  `arrival_dttm` DATETIME NULL DEFAULT NULL,
  `close_dttm` DATETIME NULL DEFAULT NULL,
  `city` text,
  `zipcode` text,
  `station_area` text,
  `box` text,
  `suppression_units` int DEFAULT NULL,
  `suppression_personnel` int DEFAULT NULL,
  `ems_units` int DEFAULT NULL,
  `ems_personnel` int DEFAULT NULL,
  `other_units` int DEFAULT NULL,
  `other_personnel` int DEFAULT NULL,
  `first_unit_on_scene` text,
  `estimated_property_loss` int DEFAULT NULL,
  `estimated_contents_loss` int DEFAULT NULL,
  `fire_fatalities` int DEFAULT NULL,
  `fire_injuries` int DEFAULT NULL,
  `civilian_fatalities` int DEFAULT NULL,
  `civilian_injuries` int DEFAULT NULL,
  `number_of_alarms` int DEFAULT NULL,
  `primary_situation` text,
  `mutual_aid` text,
  `action_taken_primary` text,
  `action_taken_secondary` text,
  `action_taken_other` text,
  `detector_alerted_occupants` text,
  `property_use` text,
  `area_of_fire_origin` text,
  `ignition_cause` text,
  `ignition_factor_primary` text,
  `ignition_factor_secondary` text,
  `heat_source` text,
  `item_first_ignited` text,
  `human_factors_associated_with_ignition` text,
  `structure_type` text,
  `structure_status` text,
  `floor_of_fire_origin` int DEFAULT NULL,
  `fire_spread` text,
  `no_flame_spread` text,
  `number_of_floors_with_minimum_damage` int DEFAULT NULL,
  `number_of_floors_with_significant_damage` int DEFAULT NULL,
  `number_of_floors_with_heavy_damage` int DEFAULT NULL,
  `number_of_floors_with_extreme_damage` int DEFAULT NULL,
  `detectors_present` text,
  `detector_type` text,
  `detector_operation` text,
  `detector_effectiveness` text,
  `detector_failure_reason` text,
  `automatic_extinguishing_system_present` text,
  `automatic_extinguishing_sytem_type` text,
  `automatic_extinguishing_sytem_perfomance` text,
  `automatic_extinguishing_sytem_failure_reason` text,
  `number_of_sprinkler_heads_operating` int DEFAULT NULL,
  `supervisor_district` text,
  `point` text,
  `data_as_of` DATETIME NULL DEFAULT NULL,
  `data_loaded_at` DATETIME NULL DEFAULT null,
  foreign key(time_period_id) references incidents.dim_time_period(time_period_id),
  foreign key(battalion_id) references incidents.dim_battalion(battalion_id),
  foreign key(id_district) references incidents.dim_district(id_district)
  
 );


#Insertion into fact table
INSERT INTO incidents.fire_incidents_fact(
	incident_number,
	 exposure_number,
	 id,
	  address,
	  incident_date,
	  call_number,
	  alarm_dttm,
	  arrival_dttm,
	  close_dttm,
	  city,
	  zipcode,
	  station_area,
	  box,
	  suppression_units,
	  suppression_personnel,
	  ems_units,
	  ems_personnel,
	  other_units,
	  other_personnel,
	  first_unit_on_scene,
	  estimated_property_loss,
	  estimated_contents_loss,
	  fire_fatalities,
	  fire_injuries,
	  civilian_fatalities,
	  civilian_injuries,
	  number_of_alarms,
	  primary_situation,
	  mutual_aid,
	  action_taken_primary,
	  action_taken_secondary,
	  action_taken_other,
	  detector_alerted_occupants,
	  property_use,
	  area_of_fire_origin,
	  ignition_cause,
	  ignition_factor_primary,
	  ignition_factor_secondary,
	  heat_source,
	  item_first_ignited,
	  human_factors_associated_with_ignition,
	  structure_type,
	  structure_status,
	  floor_of_fire_origin,
	  fire_spread,
	  no_flame_spread,
	  number_of_floors_with_minimum_damage,
	  number_of_floors_with_significant_damage,
	  number_of_floors_with_heavy_damage,
	  number_of_floors_with_extreme_damage,
	  detectors_present,
	  detector_type,
	  detector_operation,
	  detector_effectiveness,
	  detector_failure_reason,
	  automatic_extinguishing_system_present,
	  automatic_extinguishing_sytem_type,
	  automatic_extinguishing_sytem_perfomance,
	  automatic_extinguishing_sytem_failure_reason,
	  number_of_sprinkler_heads_operating,
	  supervisor_district,
	  point,
	  data_as_of,
	  data_loaded_at,
	  time_period_id,
	  id_district,
	  battalion_id
  )
select incident_number,
 exposure_number,
 id,
  address,
  incident_date,
  call_number,
  alarm_dttm,
  arrival_dttm,
  close_dttm,
  city,
  zipcode,
  station_area,
  box,
  suppression_units,
  suppression_personnel,
  ems_units,
  ems_personnel,
  other_units,
  other_personnel,
  first_unit_on_scene,
  estimated_property_loss,
  estimated_contents_loss,
  fire_fatalities,
  fire_injuries,
  civilian_fatalities,
  civilian_injuries,
  number_of_alarms,
  primary_situation,
  mutual_aid,
  action_taken_primary,
  action_taken_secondary,
  action_taken_other,
  detector_alerted_occupants,
  property_use,
  area_of_fire_origin,
  ignition_cause,
  ignition_factor_primary,
  ignition_factor_secondary,
  heat_source,
  item_first_ignited,
  human_factors_associated_with_ignition,
  structure_type,
  structure_status,
  floor_of_fire_origin,
  fire_spread,
  no_flame_spread,
  number_of_floors_with_minimum_damage,
  number_of_floors_with_significant_damage,
  number_of_floors_with_heavy_damage,
  number_of_floors_with_extreme_damage,
  detectors_present,
  detector_type,
  detector_operation,
  detector_effectiveness,
  detector_failure_reason,
  automatic_extinguishing_system_present,
  automatic_extinguishing_sytem_type,
  automatic_extinguishing_sytem_perfomance,
  automatic_extinguishing_sytem_failure_reason,
  number_of_sprinkler_heads_operating,
  supervisor_district,
  point,
  data_as_of,
  data_loaded_at,
  alarm.time_period_id,
  distr.id_district,
  battal.battalion_id
from incidents.fire_incidents_tmp as tmp
inner join incidents.dim_time_period as alarm
on hour(tmp.alarm_dttm) = alarm.alarm_hour and 
MINUTE(tmp.alarm_dttm) = alarm.alarm_min and 
SECOND(tmp.alarm_dttm) = alarm.alarm_sec
inner join incidents.dim_district as distr
on tmp.neighborhood_district = distr.neighborhood_district 
inner join incidents.dim_battalion battal
on tmp.battalion =  battal.battalion;




# The battalion that responded to the most incidents
select battalion,count(*) as total from incidents.fire_incidents_fact  as fact
inner join incidents.dim_battalion as batta
on fact.battalion_id = batta.battalion_id
group by battalion order by count(*) desc;


#The district with the most incidents
select neighborhood_district,count(*) as total from incidents.fire_incidents_fact  as fact
inner join incidents.dim_district as distri
on fact.id_district = distri.id_district
group by neighborhood_district order by count(*) desc;

#District most served by each battalion
select battalion,neighborhood_district,count(*) as total from incidents.fire_incidents_fact  as fact
inner join incidents.dim_battalion as batta
on fact.battalion_id = batta.battalion_id
inner join incidents.dim_district as distri
on fact.id_district = distri.id_district
group by battalion,neighborhood_district order by count(*) desc;


#Total incidents per hour
select alarm_hour,count(*) as total from incidents.fire_incidents_fact  as fact
inner join incidents.dim_district as distri
on fact.id_district = distri.id_district
inner join incidents.dim_time_period as alarm
on fact.time_period_id = alarm.time_period_id
group by alarm_hour;






















