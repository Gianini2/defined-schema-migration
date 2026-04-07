-- I slighty changed the DDL to adapt for PostgreSQL

CREATE TABLE IF NOT EXISTS monument.facility (
    facilityId SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS monument.unit (
	unitId INT PRIMARY KEY,
	facilityId INT NOT NULL,
	number VARCHAR(10) NOT NULL,
	unitWidth FLOAT,
	unitLength FLOAT,
	unitHeight FLOAT,
	unitType VARCHAR(20),
	monthlyRent DECIMAL(10,2),
	FOREIGN KEY (facilityId) REFERENCES monument.facility(facilityId)
);

CREATE TABLE IF NOT EXISTS monument.tenant (
	tenantId SERIAL PRIMARY KEY,
	firstName VARCHAR(50),
	lastName VARCHAR(50),
	email VARCHAR(100),
	phone VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS monument."rentalContract" (
	rentalContractId INT PRIMARY KEY,
	unitId INT NOT NULL,
	tenantId INT NOT NULL,
	startDate TIMESTAMP NOT NULL,
	endDate TIMESTAMP,
	currentAmountOwed DECIMAL(10,2), --sum of rentalInvoice.invoiceBalance
	FOREIGN KEY (unitId) REFERENCES monument.unit(unitId),
	FOREIGN KEY (tenantId) REFERENCES monument.tenant(tenantId)
);

CREATE TABLE IF NOT EXISTS monument."rentalInvoice" (
	invoiceId INT PRIMARY KEY,
	rentalContractId INT NOT NULL,
	invoiceDueDate TIMESTAMP NOT NULL,
	invoiceAmount DECIMAL(10,2), 	--amount invoiced
	invoiceBalance DECIMAL(10,2), 	--amount owed
	FOREIGN KEY (rentalContractId) REFERENCES monument."rentalContract"(rentalContractId)
);