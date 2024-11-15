-- Institutions Table (from IPEDS)
CREATE TABLE Institutions (
    UNITID INT PRIMARY KEY,                       -- Unit ID
    INSTNM VARCHAR(255),                          -- Institution Name
    ADDR VARCHAR(255),                            -- Address
    CITY VARCHAR(100),                            -- City
    STABBR CHAR(2),                               -- State Abbreviation
    ZIP VARCHAR(10),                              -- Zip Code
    LATITUDE DECIMAL(8, 6),                       -- Latitude
    LONGITUD DECIMAL(9, 6),                       -- Longitude
    CONTROL VARCHAR(20),                          -- Control Type
    OBEREG VARCHAR(50),                           -- Region
    CCBASIC VARCHAR(50),                          -- Carnegie Classification
    CBSA INT,                                     -- Core-Based Statistical Area
    CSA INT,                                      -- Combined Statistical Area
    COUNTYCD VARCHAR(5)                           -- County FIPS Code
);

-- Annual College Scoreboard Data
CREATE TABLE College_SB_Annual (
    UNITID INT,                                   -- Unit ID
    YEAR INT NOT NULL,                            -- Year
    ACCREDAGENCY VARCHAR(255),                    -- Accreditation Agency
    PREDDEG VARCHAR(100),                         -- Predominant Degree Awarded
    HIGHDEG VARCHAR(100),                         -- Highest Degree Awarded
    ADM_RATE DECIMAL(6, 4),                       -- Admission Rate
    C150_4 DECIMAL(6, 4),                         -- 4-Year Graduation Rate
    C200_4 DECIMAL(6, 4),                         -- 6-Year Graduation Rate
    AVGFACSAL MONEY CHECK (AVGFACSAL > 0::money), -- Average Faculty Salary
    PRIMARY KEY (UNITID, YEAR),
    FOREIGN KEY (UNITID) REFERENCES Institutions(UNITID)
);

-- Financial_Data Table (College Scorecard)
CREATE TABLE Financial_Data (
    OPEID INT,                                    -- OPE ID
    UNITID INT,                                   -- Unit ID
    YEAR INT,                                     -- Year
    TUITIONFEE_IN MONEY CHECK(TUITIONFEE_IN > 0::money), -- In-State Tuition
    TUITIONFEE_OUT MONEY CHECK(TUITIONFEE_OUT > 0::money), -- Out-of-State Tuition
    TUITIONFEE_PROG MONEY CHECK(TUITIONFEE_PROG > 0::money), -- Program-Specific Tuition
    NPT4_PUB MONEY CHECK(NPT4_PUB >= 0::money),   -- Average Net Price for Public Institutions
    PCTPELL DECIMAL(6,4),                         -- Pell Grant Rate
    DEBT_MDN MONEY CHECK(DEBT_MDN >= 0::money),   -- Median Debt
    RPY_3YR_RT DECIMAL,                           -- 3-Year Loan Repayment Rate
    CDR2 DECIMAL,                                 -- 2-Year Cohort Default Rate
    CDR3 DECIMAL,                                 -- 3-Year Cohort Default Rate
    MD_EARN_WNE_P8 MONEY CHECK(MD_EARN_WNE_P8 >= 0::money), -- Median Earnings 8 Years After Entry
    PRIMARY KEY (UNITID, YEAR),
    FOREIGN KEY (UNITID) REFERENCES InstitutionsTest2(UNITID)
);

-- Crosswalk Table
CREATE TABLE Crosswalks (
    UNITID INT,                                   
    OPEID INT,                                 
    PRIMARY KEY (UNITID, OPEID),
    FOREIGN KEY (UNITID) REFERENCES InstitutionsTest2(UNITID)
);

select * from Institutions
LIMIT 10;

select * from College_SB_Annual
LIMIT 10;

select * from Financial_Data
LIMIT 10;

select * from Crosswalks
LIMIT 10;
