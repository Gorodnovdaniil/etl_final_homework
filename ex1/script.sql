DROP TABLE used_cars;

CREATE TABLE used_cars (
    Brand TEXT,
    Model TEXT,
    Year INT32,
    Mileage_kmpl DOUBLE,
    Engine_CC DOUBLE,
    Horsepower DOUBLE,
    Fuel_Type TEXT,
    Transmission TEXT,
    Owner_Type TEXT,
    Color TEXT,
    City TEXT,
    Kms_Driven DOUBLE,
    Insurance_Valid INT32,
    Service_History INT32,
    Accidents DOUBLE,
    Tax_Paid INT32,
    Number_of_Doors DOUBLE,
    Seats DOUBLE,
    Registration_Age DOUBLE,
    Price DOUBLE,
    PRIMARY KEY (Brand, Model)
);
