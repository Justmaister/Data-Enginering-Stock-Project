# Data-Enginering-Stock-Project

## Project Description
- This Project is a case study for a start-up, that track the evolution of the stock ratios as well las the financial statements of the stock companies.
- The aim of this project is to create an **ETL procces with AWS cloud resources**, on the stock data, to answer some different questions that an investing company could have in order to take decisions more accurated.
- The data and metadata about the stoks comes from a Stock web page that conatins information realted with the stock market (news, stock prices, financial statements, and a lot more) called [Financial Modeling Prep] that has an **Rest API** with a lot of stock information.

## Architecture

The technical architecture for this project is as show below:

![Architecture](https://github.com/Justmaister/Data-Enginering-Stock-Project/blob/master/images/Finance%20Project%20Architecture.png)

1. The Data extraction is done using the Rest API to the Financial Modeling Prep using the GET request.
```sh
https://financialmodelingprep.com/api/v3/income-statement/AAPL?&apikey=demo
```
2. Copy the data downloaded from the API call to an S3 bucket (Staging)
3. Run the ETL pipeline, proces the Data and ingest it to a Redshift Cluster (Data Warehouse) for analytical purposes.

## Choice of Technologies
- For orchestrating the steps in the pipeline, **Airflow** is chosen as it allows building of data pipelines that are straghtforward and modular. Airflow allows tasks to be defined in a Directed Acyclic Graph (DAG) with dependencies of tasks between one another. This allows running of tasks to be optimized. It also enables the pipeline to be run on a schedule (for eg, daily) should the need arise. Finally, it has an intuitive UI that allows users to check the steps in the data pipeline should any part of the pipeline fail.
- To store all the data in raw format **S3** is chossen to acomplish that task as it is the Data Lake of AWS the cloud infrastructure used in this project.
- **Redshift** is chosen as the cloud Data Warehouse as it is highly scalable. Should our data grow in size, we can provision more nodes or scale up, to handle the larger volume of data.

## Data Model

- The Data Model of the project is a Star Schema as shown below. With the Symbol_list table as a Fact table and the other tables are the dimensional tables.

![Data Model](https://github.com/Justmaister/Data-Enginering-Stock-Project/blob/master/images/Data%20Model.png)

These diagram is only and approach with some columns in each table, it don't show all the columns that the table contains because there are some table containing more than 30 columns.

The Goal of this Data Enginering Project is to compare the historical evolution of the company stock and compare between them. The Data Analyst team will run queries comparing the stocks performance by the market of the company, by their sector which they operate. There is the possibility to do that in a BI tool to take the decisions more acurated.

## ETL Pipeline

The ETL process runs through an Airflow DAG:

![ETL Pipeline](https://github.com/Justmaister/Data-Enginering-Stock-Project/blob/master/images/ETL%20Pipeline.PNG)

The process is as follows:

- We call the API to get the data and save it to S3
- Create the redshift cluster
- Create the staging Tables on Redshift
- Copy data to Staging Tables
- Create the final Tables on Redshift
- Upsert data into final Tables
- Run quality check on the final tables

![DAG Execution](https://github.com/Justmaister/Data-Enginering-Stock-Project/blob/master/images/DAG%20Execution.PNG)

## Potential Improvements

The assumption that I have made is that the data volume will not increase subtantially and the pipeline is only required to run once

1. What if data is increased by 100x?

We can create an EMR resource on AWS to run on spark cluster and scale it if necesary. Furthermore, airflow schedules can be utilized to pull only a subset of the data at a time, to reduce volume of data handled at any one time.

2. What if data pipeline needs to be run by 7am daily?

We can turn on the EC2 machine and run the pipeline before 7am daily. Currently, the schedule of the airflow pipeline is set to ingest only once. We can set it to a daily schedule, to ingest new data coming in daily.

3. What if the database needs to be accessed by 100+ users?

Redshift should not have an issue handling many users, but we should be careful to scale up/scale out with more nodes whenever necessary. To provide efficiency to queries, we can seek to understand common queries users have, so we can tweak our data model. Aggregated data tables can be provided beforehand to reduce query times. We can also assign sort keys according to users querying needs for each table.

## Data Dictionary

#### Fact  Tables

**symbol_list**
| Column | Type | Constaint |
| --- | --- | --- |
| symbol | varchar (10) | Primary Key
| name | varchar (256)
| price | double precision
| exchange | varchar (40)


#### Dimension tables

**balance_sheet**
| Column | Type | Constaint |
| --- | --- | --- |
| date | date | Composite Key|
|symbol | varchar (10) | Composite Key|
|fillingDate | timestamp|
|acceptedDate | timestamp|
|period | varchar(10) | Coomposite Key|
|cashAndCashEquivalents | bigint|
shortTermInvestments | bigint|
cashAndShortTermInvestments | bigint|
netReceivables | bigint|
inventory | bigint|
otherCurrentAssets | bigint|
totalCurrentAssets | bigint|
propertyPlantEquipmentNet | bigint|
goodwill | bigint|
intangibleAssets | bigint|
goodwillAndIntangibleAssets | bigint|
longTermInvestments | bigint|
taxAssets | bigint|
otherNonCurrentAssets | bigint|
totalNonCurrentAssets | bigint|
otherAssets | bigint|
totalAssets | bigint|
accountPayables | bigint|
shortTermDebt | bigint|
taxPayables | bigint|
deferredRevenue | bigint|
otherCurrentLiabilities | bigint|
totalCurrentLiabilities | bigint|
longTermDebt | bigint|
deferredRevenueNonCurrent | bigint|
deferredTaxLiabilitiesNonCurrent | bigint|
otherNonCurrentLiabilities | bigint|
totalNonCurrentLiabilities | bigint|
otherLiabilities | bigint|
totalLiabilities | bigint|
commonStock | bigint|
retainedEarnings | bigint|
accumulatedOtherComprehensiveIncomeLoss | bigint|
othertotalStockholdersEquity | bigint|
totalStockholdersEquity | bigint|
totalLiabilitiesAndStockholdersEquity | bigint|
totalInvestments | bigint|
totalDebt | bigint|
netDebt | bigint|
link | varchar(200)|
finalLink | varchar(200)|

**cash_flow**
| Column | Type | Constaint |
| --- | --- | --- |
date|date|Composite Key|
symbol|varchar(10)|Composite Key|
fillingDate|timestamp|
acceptedDate|timestamp|
period|varchar(10)|Composite Key|
netIncome|bigint|
depreciationAndAmortization|bigint|
deferredIncomeTax|bigint|
stockBasedCompensation|bigint|
changeInWorkingCapital|bigint|
accountsReceivables|bigint|
inventory|bigint|
accountsPayables|bigint|
otherWorkingCapital|bigint|
otherNonCashItems|bigint|
netCashProvidedByOperatingActivities|bigint|
investmentsInPropertyPlantAndEquipment|bigint|
acquisitionsNet|bigint|
purchasesOfInvestments|bigint|
salesMaturitiesOfInvestments|bigint|
otherInvestingActivites|bigint|
netCashUsedForInvestingActivites|bigint|
debtRepayment|bigint|
commonStockIssued|bigint|
commonStockRepurchased|bigint|
dividendsPaid|bigint|
otherFinancingActivites|bigint|
netCashUsedProvidedByFinancingActivities|bigint|
effectOfForexChangesOnCash|bigint|
netChangeInCash|bigint|
cashAtEndOfPeriod|bigint|
cashAtBeginningOfPeriod|bigint|
operatingCashFlow|bigint|
capitalExpenditure|bigint|
freeCashFlow|bigint|
link|varchar(200)|
finalLink|varchar(200)|

**income_statement**
| Column | Type | Constaint |
| --- | --- | --- |
date|date|Composite Key |
symbol|varchar(10)|Composite Key |
fillingDate|timestamp |
acceptedDate|timestamp |
period|varchar(10)|Composite Key |
revenue|bigint |
costOfRevenue|bigint |
grossProfit|bigint |
grossProfitRatio|double precision |
researchAndDevelopmentExpenses|bigint |
generalAndAdministrativeExpenses|bigint |
sellingAndMarketingExpenses|bigint |
otherExpenses|bigint |
operatingExpenses|bigint |
costAndExpenses|bigint |
interestExpense|bigint |
depreciationAndAmortization|bigint |
ebitda|bigint |
ebitdaratio|double precision |
operatingIncome|bigint |
operatingIncomeRatio|double precision |
totalOtherIncomeExpensesNet|bigint |
incomeBeforeTax|bigint |
incomeBeforeTaxRatio|double precision |
incomeTaxExpense|bigint |
netIncome|bigint |
netIncomeRatio|double precision |
eps|double precision |
epsdiluted|double precision |
weightedAverageShsOut|bigint |
weightedAverageShsOutDil|bigint |
link|varchar(200) |
finalLink|varchar(200) |

**financial_ratios**
| Column | Type | Constaint |
| --- | --- | --- |
symbol|varchar(10)|Composite Key |
date|date|Composite Key |
currentRatio|double precision |
quickRatio|double precision |
cashRatio|double precision |
daysOfSalesOutstanding|double precision |
daysOfInventoryOutstanding|double precision |
operatingCycle|double precision |
daysOfPayablesOutstanding|double precision |
cashConversionCycle|double precision |
grossProfitMargin|double precision |
operatingProfitMargin|double precision |
pretaxProfitMargin|double precision |
netProfitMargin|double precision |
effectiveTaxRate|double precision |
returnOnAssets|double precision |
returnOnEquity|double precision |
returnOnCapitalEmployed|double precision |
netIncomePerEBT|double precision |
ebtPerEbit|double precision |
ebitPerRevenue|double precision |
debtRatio|double precision |
debtEquityRatio|double precision |
longTermDebtToCapitalization|double precision |
totalDebtToCapitalization|double precision |
interestCoverage|double precision |
cashFlowToDebtRatio|double precision |
companyEquityMultiplier|double precision |
receivablesTurnover|double precision |
payablesTurnover|double precision |
inventoryTurnover|double precision |
fixedAssetTurnover|double precision |
assetTurnover|double precision |
operatingCashFlowPerShare|double precision |
freeCashFlowPerShare|double precision |
cashPerShare|double precision |
payoutRatio|double precision |
operatingCashFlowSalesRatio|double precision |
freeCashFlowOperatingCashFlowRatio|double precision |
cashFlowCoverageRatios|double precision |
shortTermCoverageRatios|double precision |
capitalExpenditureCoverageRatio|double precision |
dividendPaidAndCapexCoverageRatio|double precision |
dividendPayoutRatio|double precision |
priceBookValueRatio|double precision |
priceToBookRatio|double precision |
priceToSalesRatio|double precision |
priceEarningsRatio|double precision |
priceToFreeCashFlowsRatio|double precision |
priceToOperatingCashFlowsRatio|double precision |
priceCashFlowRatio|double precision |
priceEarningsToGrowthRatio|double precision |
priceSalesRatio|double precision |
dividendYield|double precision |
enterpriseValueMultiple|double precision |
priceFairValue|double precision |

**key_metrics**
| Column | Type | Constaint |
| --- | --- | --- |
symbol|varchar(10)|Composite Key|
date|date|Composite Key|
revenuePerShare|double precision|
netIncomePerShare|double precision|
operatingCashFlowPerShare|double precision|
freeCashFlowPerShare|double precision|
cashPerShare|double precision|
bookValuePerShare|double precision|
tangibleBookValuePerShare|double precision|
shareholdersEquityPerShare|double precision|
interestDebtPerShare|double precision|
marketCap|double precision|
enterpriseValue|double precision|
peRatio|double precision|
priceToSalesRatio|double precision|
pocfratio|double precision|
pfcfRatio|double precision|
pbRatio|double precision|
ptbRatio|double precision|
evToSales|double precision|
enterpriseValueOverEBITDA|double precision|
evToOperatingCashFlow|double precision|
evToFreeCashFlow|double precision|
earningsYield|double precision|
freeCashFlowYield|double precision|
debtToEquity|double precision|
debtToAssets|double precision|
netDebtToEBITDA|double precision|
currentRatio|double precision|
interestCoverage|double precision|
incomeQuality|double precision|
dividendYield|double precision|
payoutRatio|double precision|
salesGeneralAndAdministrativeToRevenue|double precision|
researchAndDdevelopementToRevenue|double precision|
intangiblesToTotalAssets|double precision|
capexToOperatingCashFlow|double precision|
capexToRevenue|double precision|
capexToDepreciation|double precision|
stockBasedCompensationToRevenue|double precision|
grahamNumber|double precision|
roic|double precision|
returnOnTangibleAssets|double precision|
grahamNetNet|double precision|
workingCapital|bigint|
tangibleAssetValue|bigint|
netCurrentAssetValue|bigint|
investedCapital|bigint|
averageReceivables|double precision|
averagePayables|double precision|
averageInventory|double precision|
daysSalesOutstanding|double precision|
daysPayablesOutstanding|double precision|
daysOfInventoryOnHand|double precision|
receivablesTurnover|double precision|
payablesTurnover|double precision|
inventoryTurnover|double precision|
roe|double precision|
capexPerShare|double precision|

**enterprise_value**
| Column | Type | Constaint |
| --- | --- | --- |
symbol|varchar(10)|Composite Key|
date|date|Composite Key|
stockPrice|double precision|
numberOfShares|bigint|
marketCapitalization|bigint|
minusCashAndCashEquivalents|bigint|
addTotalDebt|bigint|
enterpriseValue|bigint|

[Financial Modeling Prep]: <https://financialmodelingprep.com/>
[alanchn31]: <https://github.com/alanchn31/Movalytics-Data-Warehouse#architecture>
[Luis Github]: <https://github.com/luishus>
[FMP DEV]: <https://financialmodelingprep.com/developer/docs/>
[CSV to PandasDataframe]: <https://gist.github.com/spitfiredd/9db5b512bd93489f1f07395b97b2f237>

[Info Airflow distribution]: <We should add a new node to our Airflow DAG, to download data using API/get request and transfer to S3. In addition, to handle heavy workloads when backdating, CeleryExecutor should be used to run processes in a distributed fashion, ensuring there is no single point of failure. Furthermore, We can make use of Airflow's SLA feature, to send alerts should pipeline not have succeeded before a certain time (for eg, 6:00am)>
