class SqlQueries:
  upsert_balance_sheet = ("""
    SELECT bss.*
    FROM balance_sheet_stg bss
    LEFT JOIN balance_sheet bs
	   ON bs.symbol = bss.symbol
       AND bs.date = bss.date
       AND bs.period = bss.period
    WHERE bs.symbol IS NULL
  """)

  upsert_cash_flow = ("""
    SELECT cfs.*
    FROM cash_flow_stg cfs
    LEFT JOIN cash_flow cf
	   ON cf.symbol = cfs.symbol
       AND cf.date = cfs.date
       AND cf.period = cfs.period
    WHERE cf.symbol IS NULL
  """)

  upsert_income_statement = ("""
    SELECT inss.*
    FROM income_statement_stg inss
    LEFT JOIN income_statement ins
	   ON ins.symbol = inss.symbol
       AND ins.date = inss.date
       AND ins.period = inss.period
    WHERE ins.symbol IS NULL
  """)

  upsert_financial_ratios = ("""
    SELECT frs.*
    FROM financial_ratios_stg frs
    LEFT JOIN financial_ratios fr
	   ON fr.symbol = frs.symbol
       AND fr.date = frs.date
    WHERE fr.symbol IS NULL
  """)

  upsert_key_metrics = ("""
    SELECT kms.*
    FROM key_metrics_stg kms
    LEFT JOIN key_metrics km
	   ON km.symbol = kms.symbol
       AND km.date = kms.date
    WHERE km.symbol IS NULL
  """)

  upsert_enterprise_value = ("""
    SELECT evs.*
    FROM enterprise_value_stg evs
    LEFT JOIN enterprise_value ev
	   ON ev.symbol = evs.symbol
       AND ev.date = evs.date
    WHERE ev.symbol IS NULL
  """)

  upsert_symbol_list = ("""
    SELECT sls.*
    FROM symbol_list_stg sls
    LEFT JOIN symbol_list sl
	   ON sl.symbol = sls.symbol
    WHERE sl.symbol IS NULL
  """)

  create_final_tables = ("""
    CREATE TABLE IF NOT EXISTS symbol_list (
      symbol varchar (10) NOT NULL,
      name varchar (256),
      price double precision,
      exchange varchar (40),
      CONSTRAINT symbol_list_pkey PRIMARY KEY (symbol)
    );

    CREATE TABLE IF NOT EXISTS balance_sheet (
      date date NOT NULL,
      symbol varchar (10) NOT NULL,
      fillingDate timestamp,
      acceptedDate timestamp,
      period varchar(10) NOT NULL,
      cashAndCashEquivalents bigint,
      shortTermInvestments bigint,
      cashAndShortTermInvestments bigint,
      netReceivables bigint,
      inventory bigint,
      otherCurrentAssets bigint,
      totalCurrentAssets bigint,
      propertyPlantEquipmentNet bigint,
      goodwill bigint,
      intangibleAssets bigint,
      goodwillAndIntangibleAssets bigint,
      longTermInvestments bigint,
      taxAssets bigint,
      otherNonCurrentAssets bigint,
      totalNonCurrentAssets bigint,
      otherAssets bigint,
      totalAssets bigint,
      accountPayables bigint,
      shortTermDebt bigint,
      taxPayables bigint,
      deferredRevenue bigint,
      otherCurrentLiabilities bigint,
      totalCurrentLiabilities bigint,
      longTermDebt bigint,
      deferredRevenueNonCurrent bigint,
      deferredTaxLiabilitiesNonCurrent bigint,
      otherNonCurrentLiabilities bigint,
      totalNonCurrentLiabilities bigint,
      otherLiabilities bigint,
      totalLiabilities bigint,
      commonStock bigint,
      retainedEarnings bigint,
      accumulatedOtherComprehensiveIncomeLoss bigint,
      othertotalStockholdersEquity bigint,
      totalStockholdersEquity bigint,
      totalLiabilitiesAndStockholdersEquity bigint,
      totalInvestments bigint,
      totalDebt bigint,
      netDebt bigint,
      link varchar(200),
      finalLink varchar(200),
      CONSTRAINT balance_sheet_pkey PRIMARY KEY (symbol, date, period)
    );

    CREATE TABLE IF NOT EXISTS cash_flow (
     date date NOT NULL,
     symbol varchar(10) NOT NULL,
     fillingDate timestamp,
     acceptedDate timestamp,
     period varchar(10) NOT NULL,
     netIncome bigint,
     depreciationAndAmortization bigint,
     deferredIncomeTax bigint,
     stockBasedCompensation bigint,
     changeInWorkingCapital bigint,
     accountsReceivables bigint,
     inventory bigint,
     accountsPayables bigint,
     otherWorkingCapital bigint,
     otherNonCashItems bigint,
     netCashProvidedByOperatingActivities bigint,
     investmentsInPropertyPlantAndEquipment bigint,
     acquisitionsNet bigint,
     purchasesOfInvestments bigint,
     salesMaturitiesOfInvestments bigint,
     otherInvestingActivites bigint,
     netCashUsedForInvestingActivites bigint,
     debtRepayment bigint,
     commonStockIssued bigint,
     commonStockRepurchased bigint,
     dividendsPaid bigint,
     otherFinancingActivites bigint,
     netCashUsedProvidedByFinancingActivities bigint,
     effectOfForexChangesOnCash bigint,
     netChangeInCash bigint,
     cashAtEndOfPeriod bigint,
     cashAtBeginningOfPeriod bigint,
     operatingCashFlow bigint,
     capitalExpenditure bigint,
     freeCashFlow bigint,
     link varchar(200),
     finalLink varchar(200),
     CONSTRAINT cash_flow_pkey PRIMARY KEY (symbol, date, period)
    );

    CREATE TABLE IF NOT EXISTS income_statement (
     date date NOT NULL,
     symbol varchar(10) NOT NULL,
     fillingDate timestamp,
     acceptedDate timestamp,
     period varchar(10) NOT NULL,
     revenue bigint,
     costOfRevenue bigint,
     grossProfit bigint,
     grossProfitRatio double precision,
     researchAndDevelopmentExpenses bigint,
     generalAndAdministrativeExpenses bigint,
     sellingAndMarketingExpenses bigint,
     otherExpenses bigint,
     operatingExpenses bigint,
     costAndExpenses bigint,
     interestExpense bigint,
     depreciationAndAmortization bigint,
     ebitda bigint,
     ebitdaratio double precision,
     operatingIncome bigint,
     operatingIncomeRatio double precision,
     totalOtherIncomeExpensesNet bigint,
     incomeBeforeTax bigint,
     incomeBeforeTaxRatio double precision,
     incomeTaxExpense bigint,
     netIncome bigint,
     netIncomeRatio double precision,
     eps double precision,
     epsdiluted double precision,
     weightedAverageShsOut bigint,
     weightedAverageShsOutDil bigint,
     link varchar(200),
     finalLink varchar(200),
     CONSTRAINT income_statement_pkey PRIMARY KEY (symbol, date, period)
    );

    CREATE TABLE IF NOT EXISTS financial_ratios (
     symbol varchar(10) NOT NULL,
     date date NOT NULL,
     currentRatio double precision,
     quickRatio double precision,
     cashRatio double precision,
     daysOfSalesOutstanding double precision,
     daysOfInventoryOutstanding double precision,
     operatingCycle double precision,
     daysOfPayablesOutstanding double precision,
     cashConversionCycle double precision,
     grossProfitMargin double precision,
     operatingProfitMargin double precision,
     pretaxProfitMargin double precision,
     netProfitMargin double precision,
     effectiveTaxRate double precision,
     returnOnAssets double precision,
     returnOnEquity double precision,
     returnOnCapitalEmployed double precision,
     netIncomePerEBT double precision,
     ebtPerEbit double precision,
     ebitPerRevenue double precision,
     debtRatio double precision,
     debtEquityRatio double precision,
     longTermDebtToCapitalization double precision,
     totalDebtToCapitalization double precision,
     interestCoverage double precision,
     cashFlowToDebtRatio double precision,
     companyEquityMultiplier double precision,
     receivablesTurnover double precision,
     payablesTurnover double precision,
     inventoryTurnover double precision,
     fixedAssetTurnover double precision,
     assetTurnover double precision,
     operatingCashFlowPerShare double precision,
     freeCashFlowPerShare double precision,
     cashPerShare double precision,
     payoutRatio double precision,
     operatingCashFlowSalesRatio double precision,
     freeCashFlowOperatingCashFlowRatio double precision,
     cashFlowCoverageRatios double precision,
     shortTermCoverageRatios double precision,
     capitalExpenditureCoverageRatio double precision,
     dividendPaidAndCapexCoverageRatio double precision,
     dividendPayoutRatio double precision,
     priceBookValueRatio double precision,
     priceToBookRatio double precision,
     priceToSalesRatio double precision,
     priceEarningsRatio double precision,
     priceToFreeCashFlowsRatio double precision,
     priceToOperatingCashFlowsRatio double precision,
     priceCashFlowRatio double precision,
     priceEarningsToGrowthRatio double precision,
     priceSalesRatio double precision,
     dividendYield double precision,
     enterpriseValueMultiple double precision,
     priceFairValue double precision,
     CONSTRAINT financial_ratios_pkey PRIMARY KEY (symbol, date)
    );

    CREATE TABLE IF NOT EXISTS key_metrics (
     symbol varchar(10) NOT NULL,
     date date NOT NULL,
     revenuePerShare double precision,
     netIncomePerShare double precision,
     operatingCashFlowPerShare double precision,
     freeCashFlowPerShare double precision,
     cashPerShare double precision,
     bookValuePerShare double precision,
     tangibleBookValuePerShare double precision,
     shareholdersEquityPerShare double precision,
     interestDebtPerShare double precision,
     marketCap double precision,
     enterpriseValue double precision,
     peRatio double precision,
     priceToSalesRatio double precision,
     pocfratio double precision,
     pfcfRatio double precision,
     pbRatio double precision,
     ptbRatio double precision,
     evToSales double precision,
     enterpriseValueOverEBITDA double precision,
     evToOperatingCashFlow double precision,
     evToFreeCashFlow double precision,
     earningsYield double precision,
     freeCashFlowYield double precision,
     debtToEquity double precision,
     debtToAssets double precision,
     netDebtToEBITDA double precision,
     currentRatio double precision,
     interestCoverage double precision,
     incomeQuality double precision,
     dividendYield double precision,
     payoutRatio double precision,
     salesGeneralAndAdministrativeToRevenue double precision,
     researchAndDdevelopementToRevenue double precision,
     intangiblesToTotalAssets double precision,
     capexToOperatingCashFlow double precision,
     capexToRevenue double precision,
     capexToDepreciation double precision,
     stockBasedCompensationToRevenue double precision,
     grahamNumber double precision,
     roic double precision,
     returnOnTangibleAssets double precision,
     grahamNetNet double precision,
     workingCapital double precision,
     tangibleAssetValue double precision,
     netCurrentAssetValue double precision,
     investedCapital double precision,
     averageReceivables double precision,
     averagePayables double precision,
     averageInventory double precision,
     daysSalesOutstanding double precision,
     daysPayablesOutstanding double precision,
     daysOfInventoryOnHand double precision,
     receivablesTurnover double precision,
     payablesTurnover double precision,
     inventoryTurnover double precision,
     roe double precision,
     capexPerShare double precision,
     CONSTRAINT key_metrics_pkey PRIMARY KEY (symbol, date)
    );

    CREATE TABLE IF NOT EXISTS enterprise_value (
     symbol varchar(10) NOT NULL,
     date date NOT NULL,
     stockPrice double precision,
     numberOfShares bigint,
     marketCapitalization bigint,
     minusCashAndCashEquivalents bigint,
     addTotalDebt bigint,
     enterpriseValue bigint,
     CONSTRAINT enterprise_value_pkey PRIMARY KEY (symbol, date)
    );
""")

  create_stg_tables = ("""
    CREATE TABLE IF NOT EXISTS symbol_list_stg (
      symbol varchar (10) NOT NULL,
      name varchar (256),
      price double precision,
      exchange varchar (40),
      CONSTRAINT symbol_list_stg_pkey PRIMARY KEY (symbol)
    );

    CREATE TABLE IF NOT EXISTS balance_sheet_stg (
      date date NOT NULL,
      symbol varchar (10) NOT NULL,
      fillingDate timestamp,
      acceptedDate timestamp,
      period varchar(10) NOT NULL,
      cashAndCashEquivalents bigint,
      shortTermInvestments bigint,
      cashAndShortTermInvestments bigint,
      netReceivables bigint,
      inventory bigint,
      otherCurrentAssets bigint,
      totalCurrentAssets bigint,
      propertyPlantEquipmentNet bigint,
      goodwill bigint,
      intangibleAssets bigint,
      goodwillAndIntangibleAssets bigint,
      longTermInvestments bigint,
      taxAssets bigint,
      otherNonCurrentAssets bigint,
      totalNonCurrentAssets bigint,
      otherAssets bigint,
      totalAssets bigint,
      accountPayables bigint,
      shortTermDebt bigint,
      taxPayables bigint,
      deferredRevenue bigint,
      otherCurrentLiabilities bigint,
      totalCurrentLiabilities bigint,
      longTermDebt bigint,
      deferredRevenueNonCurrent bigint,
      deferredTaxLiabilitiesNonCurrent bigint,
      otherNonCurrentLiabilities bigint,
      totalNonCurrentLiabilities bigint,
      otherLiabilities bigint,
      totalLiabilities bigint,
      commonStock bigint,
      retainedEarnings bigint,
      accumulatedOtherComprehensiveIncomeLoss bigint,
      othertotalStockholdersEquity bigint,
      totalStockholdersEquity bigint,
      totalLiabilitiesAndStockholdersEquity bigint,
      totalInvestments bigint,
      totalDebt bigint,
      netDebt bigint,
      link varchar(200),
      finalLink varchar(200),
      CONSTRAINT balance_sheet_stg_pkey PRIMARY KEY (symbol, date, period)
    );

    CREATE TABLE IF NOT EXISTS cash_flow_stg (
     date date NOT NULL,
     symbol varchar(10) NOT NULL,
     fillingDate timestamp,
     acceptedDate timestamp,
     period varchar(10) NOT NULL,
     netIncome bigint,
     depreciationAndAmortization bigint,
     deferredIncomeTax bigint,
     stockBasedCompensation bigint,
     changeInWorkingCapital bigint,
     accountsReceivables bigint,
     inventory bigint,
     accountsPayables bigint,
     otherWorkingCapital bigint,
     otherNonCashItems bigint,
     netCashProvidedByOperatingActivities bigint,
     investmentsInPropertyPlantAndEquipment bigint,
     acquisitionsNet bigint,
     purchasesOfInvestments bigint,
     salesMaturitiesOfInvestments bigint,
     otherInvestingActivites bigint,
     netCashUsedForInvestingActivites bigint,
     debtRepayment bigint,
     commonStockIssued bigint,
     commonStockRepurchased bigint,
     dividendsPaid bigint,
     otherFinancingActivites bigint,
     netCashUsedProvidedByFinancingActivities bigint,
     effectOfForexChangesOnCash bigint,
     netChangeInCash bigint,
     cashAtEndOfPeriod bigint,
     cashAtBeginningOfPeriod bigint,
     operatingCashFlow bigint,
     capitalExpenditure bigint,
     freeCashFlow bigint,
     link varchar(200),
     finalLink varchar(200),
     CONSTRAINT cash_flow_stg_pkey PRIMARY KEY (symbol, date, period)
    );

    CREATE TABLE IF NOT EXISTS income_statement_stg (
     date date NOT NULL,
     symbol varchar(10) NOT NULL,
     fillingDate timestamp,
     acceptedDate timestamp,
     period varchar(10) NOT NULL,
     revenue bigint,
     costOfRevenue bigint,
     grossProfit bigint,
     grossProfitRatio double precision,
     researchAndDevelopmentExpenses bigint,
     generalAndAdministrativeExpenses bigint,
     sellingAndMarketingExpenses bigint,
     otherExpenses bigint,
     operatingExpenses bigint,
     costAndExpenses bigint,
     interestExpense bigint,
     depreciationAndAmortization bigint,
     ebitda bigint,
     ebitdaratio double precision,
     operatingIncome bigint,
     operatingIncomeRatio double precision,
     totalOtherIncomeExpensesNet bigint,
     incomeBeforeTax bigint,
     incomeBeforeTaxRatio double precision,
     incomeTaxExpense bigint,
     netIncome bigint,
     netIncomeRatio double precision,
     eps double precision,
     epsdiluted double precision,
     weightedAverageShsOut bigint,
     weightedAverageShsOutDil bigint,
     link varchar(200),
     finalLink varchar(200),
     CONSTRAINT income_statement_stg_pkey PRIMARY KEY (symbol, date, period)
    );

    CREATE TABLE IF NOT EXISTS financial_ratios_stg (
     symbol varchar(10) NOT NULL,
     date date NOT NULL,
     currentRatio double precision,
     quickRatio double precision,
     cashRatio double precision,
     daysOfSalesOutstanding double precision,
     daysOfInventoryOutstanding double precision,
     operatingCycle double precision,
     daysOfPayablesOutstanding double precision,
     cashConversionCycle double precision,
     grossProfitMargin double precision,
     operatingProfitMargin double precision,
     pretaxProfitMargin double precision,
     netProfitMargin double precision,
     effectiveTaxRate double precision,
     returnOnAssets double precision,
     returnOnEquity double precision,
     returnOnCapitalEmployed double precision,
     netIncomePerEBT double precision,
     ebtPerEbit double precision,
     ebitPerRevenue double precision,
     debtRatio double precision,
     debtEquityRatio double precision,
     longTermDebtToCapitalization double precision,
     totalDebtToCapitalization double precision,
     interestCoverage double precision,
     cashFlowToDebtRatio double precision,
     companyEquityMultiplier double precision,
     receivablesTurnover double precision,
     payablesTurnover double precision,
     inventoryTurnover double precision,
     fixedAssetTurnover double precision,
     assetTurnover double precision,
     operatingCashFlowPerShare double precision,
     freeCashFlowPerShare double precision,
     cashPerShare double precision,
     payoutRatio double precision,
     operatingCashFlowSalesRatio double precision,
     freeCashFlowOperatingCashFlowRatio double precision,
     cashFlowCoverageRatios double precision,
     shortTermCoverageRatios double precision,
     capitalExpenditureCoverageRatio double precision,
     dividendPaidAndCapexCoverageRatio double precision,
     dividendPayoutRatio double precision,
     priceBookValueRatio double precision,
     priceToBookRatio double precision,
     priceToSalesRatio double precision,
     priceEarningsRatio double precision,
     priceToFreeCashFlowsRatio double precision,
     priceToOperatingCashFlowsRatio double precision,
     priceCashFlowRatio double precision,
     priceEarningsToGrowthRatio double precision,
     priceSalesRatio double precision,
     dividendYield double precision,
     enterpriseValueMultiple double precision,
     priceFairValue double precision,
     CONSTRAINT financial_ratios_stg_pkey PRIMARY KEY (symbol, date)
    );

    CREATE TABLE IF NOT EXISTS key_metrics_stg (
     symbol varchar(10) NOT NULL,
     date date NOT NULL,
     revenuePerShare double precision,
     netIncomePerShare double precision,
     operatingCashFlowPerShare double precision,
     freeCashFlowPerShare double precision,
     cashPerShare double precision,
     bookValuePerShare double precision,
     tangibleBookValuePerShare double precision,
     shareholdersEquityPerShare double precision,
     interestDebtPerShare double precision,
     marketCap double precision,
     enterpriseValue double precision,
     peRatio double precision,
     priceToSalesRatio double precision,
     pocfratio double precision,
     pfcfRatio double precision,
     pbRatio double precision,
     ptbRatio double precision,
     evToSales double precision,
     enterpriseValueOverEBITDA double precision,
     evToOperatingCashFlow double precision,
     evToFreeCashFlow double precision,
     earningsYield double precision,
     freeCashFlowYield double precision,
     debtToEquity double precision,
     debtToAssets double precision,
     netDebtToEBITDA double precision,
     currentRatio double precision,
     interestCoverage double precision,
     incomeQuality double precision,
     dividendYield double precision,
     payoutRatio double precision,
     salesGeneralAndAdministrativeToRevenue double precision,
     researchAndDdevelopementToRevenue double precision,
     intangiblesToTotalAssets double precision,
     capexToOperatingCashFlow double precision,
     capexToRevenue double precision,
     capexToDepreciation double precision,
     stockBasedCompensationToRevenue double precision,
     grahamNumber double precision,
     roic double precision,
     returnOnTangibleAssets double precision,
     grahamNetNet double precision,
     workingCapital double precision,
     tangibleAssetValue double precision,
     netCurrentAssetValue double precision,
     investedCapital double precision,
     averageReceivables double precision,
     averagePayables double precision,
     averageInventory double precision,
     daysSalesOutstanding double precision,
     daysPayablesOutstanding double precision,
     daysOfInventoryOnHand double precision,
     receivablesTurnover double precision,
     payablesTurnover double precision,
     inventoryTurnover double precision,
     roe double precision,
     capexPerShare double precision,
     CONSTRAINT key_metrics_stg_pkey PRIMARY KEY (symbol, date)
    );

    CREATE TABLE IF NOT EXISTS enterprise_value_stg (
     symbol varchar(10) NOT NULL,
     date date NOT NULL,
     stockPrice double precision,
     numberOfShares bigint,
     marketCapitalization bigint,
     minusCashAndCashEquivalents bigint,
     addTotalDebt bigint,
     enterpriseValue bigint,
     CONSTRAINT enterprise_value_stg_pkey PRIMARY KEY (symbol, date)
    );
""")

  drop_stg_tables = ("""
    DROP TABLE IF EXISTS symbol_list_stg;
    DROP TABLE IF EXISTS balance_sheet_stg;
    DROP TABLE IF EXISTS cash_flow_stg
    DROP TABLE IF EXISTS income_statement_stg;
    DROP TABLE IF EXISTS financial_ratios_stg;
    DROP TABLE IF EXISTS key_metrics_stg;
    DROP TABLE IF EXISTS enterprise_value_stg;
  """)
