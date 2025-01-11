CREATE OR REPLACE DATABASE LYNX_PROJEKT;

CREATE OR REPLACE STAGE LYNX_PROJEKT_STG;

LIST @LYNX_PROJEKT_STG;

-- file format

CREATE OR REPLACE FILE FORMAT csv
TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1;

-- vyvtorenie tabuliek

CREATE OR REPLACE TABLE Album
(
    `AlbumId` INT NOT NULL,
    `Title` NVARCHAR(160) NOT NULL,
    `ArtistId` INT NOT NULL,
    CONSTRAINT `PK_Album` PRIMARY KEY  (`AlbumId`)
);

CREATE OR REPLACE TABLE Artist
(
    `ArtistId` INT NOT NULL,
    `Name` NVARCHAR(120),
    CONSTRAINT `PK_Artist` PRIMARY KEY  (`ArtistId`)
);

CREATE OR REPLACE TABLE Customer
(
    `CustomerId` INT NOT NULL,
    `FirstName` NVARCHAR(40) NOT NULL,
    `LastName` NVARCHAR(20) NOT NULL,
    `Company` NVARCHAR(80),
    `Address` NVARCHAR(70),
    `City` NVARCHAR(40),
    `State` NVARCHAR(40),
    `Country` NVARCHAR(40),
    `PostalCode` NVARCHAR(10),
    `Phone` NVARCHAR(24),
    `Fax` NVARCHAR(24),
    `Email` NVARCHAR(60) NOT NULL,
    `SupportRepId` INT,
    CONSTRAINT `PK_Customer` PRIMARY KEY  (`CustomerId`)
);
 
CREATE OR REPLACE TABLE Employee
(
    `EmployeeId` INT NOT NULL,
    `LastName` NVARCHAR(20) NOT NULL,
    `FirstName` NVARCHAR(20) NOT NULL,
    `Title` NVARCHAR(30),
    `ReportsTo` INT,
    `BirthDate` DATETIME,
    `HireDate` DATETIME,
    `Address` NVARCHAR(70),
    `City` NVARCHAR(40),
    `State` NVARCHAR(40),
    `Country` NVARCHAR(40),
    `PostalCode` NVARCHAR(10),
    `Phone` NVARCHAR(24),
    `Fax` NVARCHAR(24),
    `Email` NVARCHAR(60),
    CONSTRAINT `PK_Employee` PRIMARY KEY  (`EmployeeId`)
);

CREATE OR REPLACE TABLE Genre
(
    `GenreId` INT NOT NULL,
    `Name` NVARCHAR(120),
    CONSTRAINT `PK_Genre` PRIMARY KEY  (`GenreId`)
);

CREATE OR REPLACE TABLE Invoice
(
    `InvoiceId` INT NOT NULL,
    `CustomerId` INT NOT NULL,
    `InvoiceDate` DATETIME NOT NULL,
    `BillingAddress` NVARCHAR(70),
    `BillingCity` NVARCHAR(40),
    `BillingState` NVARCHAR(40),
    `BillingCountry` NVARCHAR(40),
    `BillingPostalCode` NVARCHAR(10),
    `Total` NUMERIC(10,2) NOT NULL,
    CONSTRAINT `PK_Invoice` PRIMARY KEY  (`InvoiceId`)
);

CREATE OR REPLACE TABLE InvoiceLine
(
    `InvoiceLineId` INT NOT NULL,
    `InvoiceId` INT NOT NULL,
    `TrackId` INT NOT NULL,
    `UnitPrice` NUMERIC(10,2) NOT NULL,
    `Quantity` INT NOT NULL,
    CONSTRAINT `PK_InvoiceLine` PRIMARY KEY  (`InvoiceLineId`)
);

CREATE OR REPLACE TABLE MediaType
(
    `MediaTypeId` INT NOT NULL,
    `Name` NVARCHAR(120),
    CONSTRAINT `PK_MediaType` PRIMARY KEY  (`MediaTypeId`)
);

CREATE OR REPLACE TABLE Playlist
(
    `PlaylistId` INT NOT NULL,
    `Name` NVARCHAR(120),
    CONSTRAINT `PK_Playlist` PRIMARY KEY  (`PlaylistId`)
);

CREATE OR REPLACE TABLE PlaylistTrack
(
    `PlaylistId` INT NOT NULL,
    `TrackId` INT NOT NULL,
    CONSTRAINT `PK_PlaylistTrack` PRIMARY KEY  (`PlaylistId`, `TrackId`)
);

CREATE OR REPLACE TABLE Track
(
    `TrackId` INT NOT NULL,
    `Name` NVARCHAR(200) NOT NULL,
    `AlbumId` INT,
    `MediaTypeId` INT NOT NULL,
    `GenreId` INT,
    `Composer` NVARCHAR(220),
    `Milliseconds` INT NOT NULL,
    `Bytes` INT,
    `UnitPrice` NUMERIC(10,2) NOT NULL,
    CONSTRAINT `PK_Track` PRIMARY KEY  (`TrackId`)
);

-- naplnenie tabuliek

COPY INTO Album
FROM @LYNX_PROJEKT_STG/Album.csv
FILE_FORMAT = csv
ON_ERROR = CONTINUE;

COPY INTO Artist
FROM @LYNX_PROJEKT_STG/Artist.csv
FILE_FORMAT = csv
ON_ERROR = CONTINUE;

COPY INTO Customer
FROM @LYNX_PROJEKT_STG/Customer.csv
FILE_FORMAT = csv
ON_ERROR = CONTINUE;

COPY INTO Employee
FROM @LYNX_PROJEKT_STG/Employee.csv
FILE_FORMAT = csv
ON_ERROR = CONTINUE;

COPY INTO Genre
FROM @LYNX_PROJEKT_STG/Genre.csv
FILE_FORMAT = csv
ON_ERROR = CONTINUE;

COPY INTO Invoice
FROM @LYNX_PROJEKT_STG/Invoice.csv
FILE_FORMAT = csv
ON_ERROR = CONTINUE;

COPY INTO InvoiceLine
FROM @LYNX_PROJEKT_STG/InvoiceLine.csv
FILE_FORMAT = csv
ON_ERROR = CONTINUE;

COPY INTO MediaType
FROM @LYNX_PROJEKT_STG/MediaType.csv
FILE_FORMAT = csv
ON_ERROR = CONTINUE;

COPY INTO PlayList
FROM @LYNX_PROJEKT_STG/PlayList.csv
FILE_FORMAT = csv
ON_ERROR = CONTINUE;

COPY INTO PlayListTrack
FROM @LYNX_PROJEKT_STG/PlayListTrack.csv
FILE_FORMAT = csv
ON_ERROR = CONTINUE;

COPY INTO Track
FROM @LYNX_PROJEKT_STG/Track.csv
FILE_FORMAT = csv
ON_ERROR = CONTINUE;

-- Vytvorenie dimenzii

CREATE TABLE dimInvoice AS
SELECT
    i.`INVOICEID` as dim_invoiceID,
    i.`CUSTOMERID` as dim_customerID,
    CAST(i.`INVOICEDATE` AS DATE) as invoiceDate,
    i.`BILLINGADDRESS` as dim_billingAddress,
    i.`TOTAL` as dim_total
FROM INVOICE i;


CREATE TABLE dimCustomer AS
SELECT DISTINCT
    c.`CUSTOMERID` AS dim_customerID,
    c.`FIRSTNAME` as dim_firstName,
    c.`LASTNAME` as dim_lastName,
    c.`ADDRESS` as dim_address,
    c.`PHONE` as dim_phone,
    c.`EMAIL` as dim_email,
    c.`SUPPORTREPID` as dim_supportRepID
FROM CUSTOMER c;


CREATE TABLE dimDate AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(`INVOICEDATE` AS DATE)) AS dim_dateID,
    CAST(`INVOICEDATE` AS DATE) AS date,
    DATE_PART(day, `INVOICEDATE`) AS day,
    DATE_PART(dow, `INVOICEDATE`) + 1 AS dayOfWeek,
    CASE DATE_PART(dow, `INVOICEDATE`) + 1
        WHEN 1 THEN 'Pondelok'
        WHEN 2 THEN 'Utorok'
        WHEN 3 THEN 'Streda'
        WHEN 4 THEN 'Štvrtok'
        WHEN 5 THEN 'Piatok'
        WHEN 6 THEN 'Sobota'
        WHEN 7 THEN 'Nedeľa'
    END AS dayOfWeekAsString,
    DATE_PART(month, `INVOICEDATE`) AS month,
    DATE_PART(year, `INVOICEDATE`) AS year,
    DATE_PART(quarter, `INVOICEDATE`) AS quarter
FROM INVOICE;

CREATE TABLE dimTrack AS
SELECT
    t.`TRACKID` as dim_trackID,
    t.`NAME` as dim_name,
    COALESCE(a.`TITLE`, 'Unknown') as dim_album,
    COALESCE(g.`NAME`, 'Unknown') as dim_genre,
    t.`UNITPRICE` as dim_price,
    t.`MILLISECONDS` as dim_length,
    COALESCE(ar.`NAME`, 'Unknown') as dim_artist,
    mt.`NAME` as dim_mediaType
FROM TRACK t
LEFT JOIN GENRE g ON t.`GENREID` = g.`GENREID`
LEFT JOIN ALBUM a ON t.`ALBUMID` = a.`ALBUMID`
LEFT JOIN ARTIST ar ON a.`ARTISTID` = ar.`ARTISTID`
JOIN MEDIATYPE mt ON t.`MEDIATYPEID` = mt.`MEDIATYPEID`;


CREATE TABLE dimEmployee AS
SELECT
    e.`EMPLOYEEID` as dim_employeeID,
    e.`FIRSTNAME` as dim_firstName,
    e.`LASTNAME` as dim_lastName,
    e.`TITLE` as dim_title,
    e.`REPORTSTO` as dim_managerID,
    e.`PHONE` as dim_phone,
    e.`EMAIL` as dim_email,
    e.`ADDRESS` as dim_address,
    e.`BIRTHDATE` as dim_dateOfBirth
FROM EMPLOYEE e;


CREATE TABLE InvoiceLineFact AS
SELECT
    il.`InvoiceLineId` AS invoiceLineFactID,
    il.`UnitPrice` AS dim_unitPrice,
    il.`Quantity` AS dim_quantity,
    t.DIM_TRACKID AS trackID,
    i.DIM_INVOICEID AS invoiceID,
    i.DIM_CUSTOMERID AS customerID,
    e.DIM_EMPLOYEEID AS employeeID,
    d.DIM_DATEID AS dateID
FROM InvoiceLine il
JOIN DIMTRACK t ON il.`TRACKID` = t.DIM_TRACKID
JOIN DIMINVOICE i ON il.`INVOICEID` = i.DIM_INVOICEID
JOIN DIMCUSTOMER c ON i.DIM_CUSTOMERID = c.DIM_CUSTOMERID
JOIN DIMEMPLOYEE e ON c.DIM_SUPPORTREPID = e.DIM_EMPLOYEEID
JOIN DIMDATE d ON i.INVOICEDATE = d.DATE;


DROP TABLE ALBUM;
DROP TABLE ARTIST;
DROP TABLE CUSTOMER;
DROP TABLE EMPLOYEE;
DROP TABLE GENRE;
DROP TABLE INVOICELINE;
DROP TABLE INVOICE;
DROP TABLE MEDIATYPE;
DROP TABLE PLAYLIST;
DROP TABLE PLAYLISTTRACK;
DROP TABLE TRACK;


