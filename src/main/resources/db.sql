CREATE TABLE Chair (
                         id serial primary key not null ,
                         material varchar(30),
                         country varchar(30),
                         manufacturer varchar(30),
                         color varchar(10),
                         upholstery varchar(100)
);

CREATE TABLE Tab1e (
                         id serial primary key not null ,
                         material varchar(30),
                         country varchar(30),
                         manufacturer varchar(30),
                         form varchar(50),
                         type varchar(10),
                         legs int
);