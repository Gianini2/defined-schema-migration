MERGE INTO monument."rentalContract" AS Target
USING (
	SELECT
		  rentalContractId
		, unitId
		, tenantId
		, startDate
		, endDate
		, currentAmountOwed
) AS Source
	ON Source.rentalContractId = Target.rentalContractId
WHEN MATCHED THEN
  UPDATE SET
      unitId = Source.unitId
    , tenantId = Source.tenantId
    , startDate = Source.startDate
    , endDate = Source.endDate
    , currentAmountOwed = Source.currentAmountOwed
WHEN NOT MATCHED THEN 
  INSERT 
  (
      rentalContractId
    , unitId
    , tenantId
    , startDate
    , endDate
    , currentAmountOwed)
  VALUES
  (
      Source.rentalContractId
    , Source.unitId
    , Source.tenantId
    , Source.startDate
    , Source.endDate
    , Source.currentAmountOwed
  )
WHEN NOT MATCHED BY SOURCE THEN
  DELETE
;