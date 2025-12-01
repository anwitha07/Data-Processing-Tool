CREATE DATABASE ETLJobRunner;

USE ETLJobRunner;
GO

DROP SCHEMA if EXISTS Raw;
DROP SCHEMA if EXISTS Curated;
DROP SCHEMA if EXISTS Processed;


CREATE SCHEMA Raw;
CREATE SCHEMA Curated;
CREATE SCHEMA processed;

--Config Table
CREATE TABLE Config (
	JobName VARCHAR(100) PRIMARY KEY NOT NULL,

	--Source Details
	SourceType VARCHAR(50) NOT NULL,
	SourcePath NVARCHAR(500) NULL,
	SourceSchema VARCHAR(50) NULL,
	SourceTable VARCHAR(100) NULL,

	--Target Details
	TargetSchema VARCHAR(50) NOT NULL,
	TargetTable  VARCHAR(100) NOT NULL,

	LoadType VARCHAR(50) NOT NULL,
	SCDType  VARCHAR(20) NULL
);

-- Metadata Table
CREATE TABLE Metadata(
	JobName VARCHAR(100) NOT NULL,
	SourceColumnName VARCHAR(100) NOT NULL,
	TargetColumnName VARCHAR(100) NOT NULL,
	TargetDataType VARCHAR(50) NOT NULL,
	[Length] INT NOT NULL,
	IsPK BIT DEFAULT 0,
	IsFK BIT DEFAULT 0,
	IsNullable BIT DEFAULT 1 ,
	ReferenceTable VARCHAR(100) NULL
	);

-- Audit/log Table
CREATE TABLE JobAudit(
	JobName VARCHAR(100) NOT NULL,
	Stage VARCHAR(50) NULL, -- RAW,CURATED,PROCESSED
	StartTime DATETIME DEFAULT GETDATE(),
	EndTime DATETIME NULL,
	[RowCount] INT NULL,
	[Status] VARCHAR(20) , --Success,Failed
	[Message] NVARCHAR(MAX) NULL
);
--Incremental 
CREATE TABLE IncrementalTracker(
	JobName VARCHAR(100) NOT NULL,
	Stage VARCHAR(50),
	LastLoadTime DATETIME
);

SELECT * FROM Config;
SELECT * FROM Metadata;
SELECT * FROM JobAudit;
SELECT * FROM IncrementalTracker;

--Products
SELECT * FROM raw.raw_products;
SELECT * FROM curated.curated_products;
SELECT * FROM processed.processed_products;


--Stores
SELECT * FROM raw.raw_stores;
SELECT * FROM curated.curated_stores;
SELECT * FROM processed.processed_stores;

--Transactions
SELECT * FROM raw.raw_txn;
SELECT * FROM curated.curated_txn;
SELECT * FROM processed.processed_txn;


--shipment
SELECT * FROM raw.raw_shipment;
SELECT * FROM curated.curated_shipment;
SELECT * FROM processed.processed_shipment;

--warehouse
SELECT * FROM raw.raw_warehouse;
SELECT * FROM curated.curated_warehouse;
SELECT * FROM processed.processed_warehouse;

--Suppliers
SELECT * FROM raw.raw_supplier;
SELECT * FROM curated.raw_supplier;
SELECT * FROM processed.processed_supplier;

--EMP
SELECT * FROM raw.raw_emp;
SELECT * FROM curated.curated_emp;
SELECT * FROM processed.processed_emp;
--Manager
SELECT * FROM raw.raw_manager;
SELECT * FROM curated.curated_manager;
SELECT * FROM processed.processed_manager;

---DROP Table
--EMP
DROP TABLE raw.raw_emp;
DROP TABLE raw.raw_manager;
DROP TABLE curated.curated_emp;
--Manager
DROP TABLE curated.curated_manager;
DROP TABLE processed.processed_manager;
DROP  TABLE processed.processed_emp;

--Products
DROP TABLE raw.raw_products;
DROP TABLE curated.curated_products;
DROP TABLE processed.processed_products;

--Stores
DROP TABLE raw.raw_stores;
DROP TABLE curated.curated_stores;
DROP TABLE processed.processed_stores;

--Transactions
DROP TABLE raw.raw_txn;
DROP TABLE curated.curated_txn;
DROP TABLE processed.processed_txn;

--shipment
DROP TABLE raw.raw_shipment;
DROP TABLE curated.curated_shipment;
DROP TABLE processed.processed_shipment;

--warehouse
DROP TABLE raw.raw_warehouse;
DROP TABLE curated.curated_warehouse;
DROP TABLE processed.processed_warehouse;

--Suppliers
DROP TABLE raw.raw_supplier;
DROP TABLE curated.curated_supplier;
DROP TABLE processed.processed_supplier;


TRUNCATE TABLE Config;
TRUNCATE TABLE Metadata;
TRUNCATE TABLE JobAudit;
TRUNCATE TABLE IncrementalTracker;


--Ask 
ALTER TABLE Metadata
ADD CONSTRAINT UQ_Metadata UNIQUE (JobName, TargetColumnName);

