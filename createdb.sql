create database PublicSafety;

use PublicSafety;

create table zone(
   ZONEID   INT NOT NULL,
   ZONENAME VARCHAR(150),
   CURRENT_SQUAD_NUMBER VARCHAR(50),
   VERTICES_COUNT INT ,
   ZONE_POLYGON polygon NOT NULL,
   PRIMARY KEY (ZONEID),
   SPATIAL INDEX (ZONE_POLYGON)
)ENGINE = MyISAM;



create table Officer(
   UNIQUE_BADGE_NUMBER   INT NOT NULL,
   OFFICER_NAME VARCHAR(100),
   SQUAD_NUMBER VARCHAR(50),
   OFFICER_LOCATION point NOT NULL,
   PRIMARY KEY (UNIQUE_BADGE_NUMBER),
   SPATIAL INDEX (OFFICER_LOCATION)
)ENGINE = MyISAM;




create table ROUTE(
   UNIQUE_ROUTE_NUMBER  INT NOT NULL,
   VERTICES_COUNT INT,
   ROUTE linestring NOT NULL,
   PRIMARY KEY (UNIQUE_ROUTE_NUMBER),
   SPATIAL INDEX (ROUTE) 
)ENGINE = MyISAM;


create table INCIDENT(
   UNIQUE_INCIDENT_ID  INT NOT NULL,
   INCIDENT_TYPE varchar(200),
   INCIDENT_LOCATION point NOT NULL,
   PRIMARY KEY (UNIQUE_INCIDENT_ID),
   SPATIAL INDEX (INCIDENT_LOCATION)
)ENGINE = MyISAM;

commit;

