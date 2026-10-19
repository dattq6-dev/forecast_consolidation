USE [ANALYSIS]
GO
--/****** OBJECT:  STOREDPROCEDURE [DBO].[SP_FORECAST_2026_CONSOLIDATE]    
--SET ANSI_NULLS ON
--GO
--SET QUOTED_IDENTIFIER ON
--GO

--CREATE PROCEDURE [dbo].[sp_Forecast_2026_Consolidate] (@START_FC varchar(10),@END_FC varchar(10))
--AS
--BEGIN

		DECLARE @START_FC			  VARCHAR(10) = '202601'  ----- FC period begin	-- for PBI forecast monthly
		DECLARE @END_FC				  VARCHAR(10) = '202612'  ----- FC period end

		DECLARE @REPORT_MONTH         VARCHAR(10) = @END_FC
		DECLARE @YTD_MONTH            VARCHAR(10) = @END_FC
		DECLARE @CF_VERSION           VARCHAR(20) =	'R3.2026AP'
		
---- ================= RBG ================= ----  

---- STEP 1: CREATE TABLE Budget_Unionall_LamHV
		
		IF EXISTS (SELECT TABLE_NAME FROM ANALYSIS.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Forecast_2026_UnionAll')
		DROP TABLE  ANALYSIS..Forecast_2026_UnionAll

		CREATE TABLE ANALYSIS..Forecast_2026_UnionAll
		([MONTH]		VARCHAR(12)
		,BIZLINE        VARCHAR(20)
		,CUS_TYPE		VARCHAR(20)
		,SEGMENT		VARCHAR(20)
		,CHANNEL		VARCHAR(30)  
		,SUB_CHANNEL    VARCHAR(50)
		,CUSTOMER_TIER  VARCHAR(30)
		,PERSONA		VARCHAR(30)
		,PRODUCT_FAMILY VARCHAR(90)
		,PRODUCT_LINE	VARCHAR(90)
		,SUB_PRODUCT	VARCHAR(90)
		,GROUP_MEASURE	VARCHAR(90)
		,MEASURE		VARCHAR(90)
		,SUB_MEASURE	VARCHAR(90)
		,SUB_MEASURE_2	VARCHAR(90)
		,INTERNAL		VARCHAR(50)
		,CCY			VARCHAR(10)
		,S_FLAG			VARCHAR(20)
		,INFO_1			VARCHAR(20)
		,INFO_2			VARCHAR(50)
		,[VALUE]		FLOAT
		,SOURCE_TBL     VARCHAR(100)
		)

-------------------------------------------------------------------------------------------------

-- STEP 2: CREATE BẢNG ĐÍCH

		DECLARE @i	int = 1
		DECLARE @Tbl_Name		 varchar(50)			-- Tên bảng sản phẩm
		DECLARE @Col_INTERNAL	 varchar(50)			-- Phân loại Internal: nếu là bảng Time Deposit => 'Internal' và '''' đối với các bảng sp còn lại
		DECLARE @Col_CCY		 varchar(50)			-- Phân loại Currency: Nếu bảng Time Deposit => 'CCY' và  '''' đối với các bảng sp còn lại
		DECLARE @Col_S_FLAG		 varchar(50)			-- Tên cột là S_Flag => Nếu trong bảng sản phẩm có cột này HOẶC  '1'  => Nếu không có cột đó
		DECLARE @Col_PERSONA     varchar(50)      
		DECLARE @Col_SUB_CHANNEL varchar(50)
		DECLARE @Col_INFO_1      varchar(50)
		DECLARE @Col_INFO_2	     varchar(50)

	-- Lặp qua các bảng sản phẩm:
	WHILE @i <= 13 --(SELECT COUNT(1) FROM ANALYSIS..Forecast_2026_table_name)  
	BEGIN
	
		SET @Tbl_Name		 =  (SELECT FCT_TBL_NAME FROM ANALYSIS..[2026_TBL_NAME] WHERE ROW = @i)		--- @Tbl_Name = Tên bảng tại dòng @i - cột Table_Name
		SET @Col_INTERNAL	 =  CASE WHEN @Tbl_Name LIKE '%TD' THEN 'INTERNAL' ELSE '''''' END									--- Trả về INTERNAL nếu @Tbl_Name là sp TD, other income, Trả về '' nếu là sp khác
		SET @Col_CCY		 =  CASE WHEN @Tbl_Name LIKE '%TD'	THEN 'CCY'		ELSE '''''' END												--- Trả về CCY nếu @Tbl_Name là sp TD, Trả về '' nếu là sp khác 
		SET @Col_S_FLAG		 = (SELECT ISNULL(MAX(COLUMN_NAME), '1') FROM ANALYSIS.INFORMATION_SCHEMA.COLUMNS 
											WHERE TABLE_NAME = @Tbl_Name AND COLUMN_NAME = 'S_FLAG' )									--- Nếu bảng dữ liệu có cột S_Flag --> S_Flag, nếu không --> 1
	    SET @Col_PERSONA     = (SELECT ISNULL(MAX(COLUMN_NAME),'''All''') FROM ANALYSIS.INFORMATION_SCHEMA.COLUMNS 
								WHERE TABLE_NAME = @Tbl_Name AND COLUMN_NAME = 'PERSONA' )			 --- NẾU KO CÓ CỘT PERSONA THÌ MẶC ĐỊNH PERSONA = 'All'
		SET @Col_SUB_CHANNEL = (SELECT ISNULL(MAX(COLUMN_NAME),'''All''') FROM ANALYSIS.INFORMATION_SCHEMA.COLUMNS 
								WHERE TABLE_NAME = @Tbl_Name AND COLUMN_NAME = 'SUB_CHANNEL' )			 --- NẾU KO CÓ CỘT SUB_CHANNEL THÌ MẶC ĐỊNH SUB_CHANNEL = 'All'
		SET @Col_INFO_1 = CASE WHEN @Tbl_Name LIKE '%BANCA%'		THEN 'BANCA_CHANNEL'
							   WHEN @Tbl_Name LIKE '%CASA%'			THEN 'MTB_FLAG'
							   WHEN @Tbl_Name LIKE '%CREDIT_CARD%'	THEN 'MTB_FLAG'
							   WHEN @Tbl_Name LIKE '%LENDING%'		THEN 'INFO_1'
							   WHEN @Tbl_Name LIKE '%SME_MSME%'     THEN 'INFO_1'
							   ELSE ''''''
							   END		
		SET @Col_INFO_2 = CASE WHEN @Tbl_Name LIKE '% BANCA%'		THEN 'PAYMENT_MODE'
							   --WHEN @Tbl_Name LIKE '%CASA%'			THEN '(case when AE_flag = 1 then ''AE'' else '''' end)' 
							   --WHEN @Tbl_Name LIKE '%TD%'           THEN '(case when AE_flag = 1 then ''AE'' else '''' end)'  
							   ELSE ''''''
							   END

		PRINT @Tbl_Name+'   '+@Col_INTERNAL+'   '+@Col_CCY+'   '+@Col_S_FLAG+'   '+@Col_PERSONA

        --INSERT INTO DỮ LIỆU VÀO BẢNG ĐÍCH
		-- GÁN BIẾN: @Report_Month
		EXEC('
		INSERT INTO ANALYSIS..Forecast_2026_UnionAll
		SELECT   [MONTH], BIZLINE, CUS_TYPE, SEGMENT, CHANNEL,  '+@Col_SUB_CHANNEL+', CUSTOMER_TIER, '+@Col_PERSONA+'
			   , PRODUCT_FAMILY, PRODUCT_LINE, SUB_PRODUCT, GROUP_MEASURE, MEASURE, SUB_MEASURE, SUB_MEASURE_2
			   , '+@Col_INTERNAL+' , '+@Col_CCY+' , '+@Col_S_FLAG+'
			   , '+@Col_INFO_1+' , '+@Col_INFO_2+'
			   , VALUE, '''+@Tbl_Name+'''
		FROM ANALYSIS..['+@TBL_NAME+'] 
		WHERE VALUE <> 0 AND '+@Col_S_FLAG+' <> 0
			  AND MONTH BETWEEN ''202401'' AND '''+@Report_Month+'''

		')
		 SET @i = @i + 1
	END

		EXEC('
		INSERT INTO ANALYSIS..Forecast_2026_UnionAll
		SELECT   [MONTH], BIZLINE, CUS_TYPE, SEGMENT, CHANNEL,  '+@Col_SUB_CHANNEL+', CUSTOMER_TIER, PERSONA
			   , PRODUCT_FAMILY, PRODUCT_LINE, SUB_PRODUCT, GROUP_MEASURE, MEASURE, SUB_MEASURE, SUB_MEASURE_2
			   , '''' INTERNAL , '''' CCY , S_FLAG
			   , '''' INFO_1 , INFO_2
			   , VALUE, ''RHE'' SOURCE_TBL   
		FROM ANALYSIS..Forecast_2026_RHE
		WHERE VALUE <>0 AND S_FLAG <> 0
			  AND MONTH BETWEEN ''202401'' AND '''+@Report_Month+'''
		')

---- STEP 2: CLEAN DATA & UPDATE FORMAT (if any)
	----  DELETE UNUSED DATA 

	DELETE FROM ANALYSIS..FORECAST_2026_UNIONALL
	WHERE			MEASURE IN ('EOP Ticket size','%UNIQUE CUS HAVING NEW CONTRACT - BOND','%Unique Cus Having New Contract - All','% Active cus'
							   ,'% CI','% CI_adj','% EOP bal/Avg bal','% FTP','% NIM','Avg bal/Active Cus','BOP Cus','NFI/ active cus','Number TNX/Active Cus')
			OR (	MEASURE IN ('UNIQUE CUS HAVING NEW CONTRACT') AND SUB_PRODUCT IN ('BOND','FUND')	)


	----  UPDATE FORMAT
	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET   GROUP_MEASURE		= 'EOP Cus'	
		, MEASURE			= 'EOP Cus'	
		, SUB_MEASURE		= 'EOP Cus'	
		, SUB_MEASURE_2		= 'EOP Cus'	
	WHERE BIZLINE = 'Retail' AND PRODUCT_LINE IN ('BOND-FUND','Structured Notes') AND SUB_PRODUCT IN ('BOND-FUND','Structured Notes' ) AND MEASURE ='EOP Cus by product'

	UPDATE  ANALYSIS..FORECAST_2026_UNIONALL		 
	SET  INFO_2 = REPLACE(INFO_2,'0','')	
	WHERE  PRODUCT_LINE ='BANCA'  AND INFO_2 LIKE '%0%'   

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET  MEASURE		= GROUP_MEASURE
		,SUB_MEASURE	= MEASURE
		,SUB_MEASURE_2  = SUB_MEASURE	---- Details of Card FI/FE
	WHERE PRODUCT_FAMILY ='CARD' AND GROUP_MEASURE = 'NFI' AND MEASURE IN ('FI','FE') 

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET  MEASURE		= GROUP_MEASURE
		,SUB_MEASURE	= MEASURE
		,SUB_MEASURE_2  = SUB_MEASURE	---- Details of BOND-FUND NFI
	WHERE PRODUCT_LINE ='BOND-FUND' AND GROUP_MEASURE = 'NFI'

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET  SUB_PRODUCT	= 'Prix'
	WHERE PRODUCT_LINE ='BOND-FUND' AND SUB_MEASURE = 'Incentive Fee' AND SUB_MEASURE_2 ='Secondary Fee' AND SUB_PRODUCT ='Others'

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET  SUB_MEASURE	= 'CI'
		,SUB_MEASURE_2  = 'Liquidity Cost'	---- Liquidity Cost
	WHERE PRODUCT_FAMILY ='Time Deposit' AND GROUP_MEASURE = 'NII' AND SUB_MEASURE IN ('Liquidity Cost','Liquidity_Cost') 

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET   SUB_MEASURE	= 'FI'	
		 ,SUB_MEASURE_2 = 'FI'	
	WHERE PRODUCT_FAMILY IN ('Time Deposit','Lending') AND SUB_MEASURE ='NFI' 

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET   MEASURE	= 'NII'	
	WHERE PRODUCT_FAMILY IN ('LENDING') AND GROUP_MEASURE ='NII' AND MEASURE <> 'NII'

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET   MEASURE	= 'NFI'	
	WHERE PRODUCT_FAMILY IN ('LENDING','CARD') AND GROUP_MEASURE ='NFI' AND MEASURE <> 'NFI'

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET   GROUP_MEASURE		= 'EOP Cus'	
			,MEASURE		= 'EOP Cus'	
			,SUB_MEASURE	= 'EOP Cus'	
			,SUB_MEASURE_2	= 'EOP Cus'	
	WHERE PRODUCT_FAMILY IN ('Time Deposit') AND PRODUCT_LINE = 'ALL' AND MEASURE ='EOP Cus Distinct'

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET   CCY = ''	WHERE CCY ='All' 

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET   CUSTOMER_TIER = 'Others'	WHERE CUSTOMER_TIER ='All' OR CUSTOMER_TIER IS NULL OR CUSTOMER_TIER ='NULL'

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET   CHANNEL = 'Others'	WHERE CHANNEL = 'ALL' OR CHANNEL NOT IN ('BRANCH','DEVELOPER','PARTNERSHIP','DIGITAL','WINLIFE','OTHERS')

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET   PRODUCT_LINE = 'Other Loans', SUB_PRODUCT = 'Other Loans'	WHERE SUB_PRODUCT IN ('Loans to CIs')

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET   CUS_TYPE = 'Existing'	WHERE CUS_TYPE ='ETB'
	
	UPDATE ANALYSIS..BUDGET_2026_UNIONALL --- UPdate tạm ngày 26/12
	SET SEGMENT = 'Others'	WHERE SEGMENT IS NULL
	
	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET   PRODUCT_FAMILY = 'QR Merchant'	WHERE PRODUCT_LINE = 'QR Merchant'                          

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL													  
	SET SUB_PRODUCT = 'Other Trading Income' WHERE PRODUCT_LINE = 'Trading Income' 
	--UPDATE IN 20251122
	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET MEASURE = 'Ticket size FX', SUB_MEASURE = 'Ticket size FX' WHERE PRODUCT_LINE = 'FX Flow' AND BIZLINE = 'Retail'

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET SUB_MEASURE_2 = 'MTB Cus' WHERE PRODUCT_LINE = 'CASA&AE' AND MEASURE = 'EOP Cus' AND INFO_1 = 'MTB'  AND BIZLINE = 'Retail'

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET SUB_MEASURE_2 = 'Truly Active Cus' WHERE PRODUCT_LINE = 'CASA&AE' AND MEASURE = 'EOP Cus' AND INFO_1 = 'Truly Active'  AND BIZLINE = 'Retail'

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET SUB_MEASURE_2 = 'Low Active Cus' WHERE PRODUCT_LINE = 'CASA&AE' AND MEASURE = 'EOP Cus' AND INFO_1 = 'Low Active'  AND BIZLINE = 'Retail'

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET SUB_PRODUCT = 'CDST' WHERE SUB_PRODUCT = 'CD Self trade'  AND BIZLINE = 'Retail'

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET PRODUCT_FAMILY = 'Fee base' WHERE PRODUCT_FAMILY = 'NFX' AND BIZLINE = 'RETAIL'

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET PRODUCT_LINE = 'NFX' WHERE PRODUCT_LINE = 'FX Flow' AND BIZLINE = 'RETAIL'

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET   MEASURE = 'TXN Amt' , SUB_MEASURE	= 'TXN Amt'	, SUB_MEASURE_2	= 'TXN Amt'	
	WHERE  BIZLINE = 'Retail' AND MEASURE ='Txn_amt' and product_line = 'nfx'

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET   MEASURE = 'Ticket size FX' , SUB_MEASURE	= 'Ticket size FX'	, SUB_MEASURE_2	= 'Ticket size FX'	
	WHERE  BIZLINE = 'Retail' AND MEASURE ='Ticket size' and product_line = 'NFX'

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET   MEASURE = 'EOP Bal' , SUB_MEASURE	= 'EOP Bal'	, SUB_MEASURE_2	= 'EOP Bal'	
	WHERE  BIZLINE = 'Retail' AND MEASURE ='AUM' and product_line = 'DPM'

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET SUB_MEASURE_2 = 'NFI'
	WHERE PRODUCT_LINE = 'Bond-Fund' and MEASURE = 'NFI'

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET SUB_MEASURE = 'FI'
	WHERE PRODUCT_LINE = 'Other Fee' and MEASURE = 'NFI'

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET PRODUCT_LINE  = 'NFX'
	WHERE PRODUCT_LINE = 'FX Flow' 

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL
	SET MEASURE  = 'Active Cus', SUB_MEASURE  = 'Active Cus', SUB_MEASURE_2  = 'Active Cus' 
	WHERE SUB_MEASURE = 'EOP Cus Active' AND BIZLINE = 'SME'

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL   -- UPDATE 20260226
	SET SUB_MEASURE = 'FI'
	WHERE SUB_MEASURE = 'NFI' AND PRODUCT_LINE IN ('Bond','Bond-Fund')

	UPDATE ANALYSIS..FORECAST_2026_UNIONALL   -- UPDATE 20260226
	SET PRODUCT_LINE = 'CASA'
	WHERE GROUP_MEASURE = 'NFI' AND PRODUCT_LINE IN ('CA') AND BIZLINE = 'SME'

	---- STEP 3: BACKUP

	DECLARE @DATE_REPORT VARCHAR(10) = (SELECT CONVERT(VARCHAR(10), GETDATE(), 112))

	EXEC('
		IF EXISTS (SELECT TABLE_NAME FROM ANALYSIS.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME=''FORECAST_2026_UNIONALL_'+@DATE_REPORT+''')
		DROP TABLE ANALYSIS..Forecast_2026_UnionAll_'+@DATE_REPORT+'

		SELECT * INTO ANALYSIS..Forecast_2026_UnionAll_'+@DATE_REPORT+' FROM ANALYSIS..Forecast_2026_UnionAll
	')

	--PRINT 'BACKUP DONE'

	---- STEP 4: CALCULATION FOR EXCEL SHEETS 
	
		-- DECLARE @REPORT_MONTH		  VARCHAR(20)	 = '202612'										-- GÁN BIẾN: @Report_Month =  THÁNG FC
		DECLARE @TBL_BUDGET_2026		  VARCHAR(100) = 'ANALYSIS..BUDGET_2026_UNIONALL'				-- BẢNG BUDGET ORIGINAL
		DECLARE @TBL_FORECAST_2026		  VARCHAR(100) = 'ANALYSIS..FORECAST_2026_UNIONALL'			    -- BẢNG FORECAST
		DECLARE @TBL_FORECAST_LM          VARCHAR(100) = 'ANALYSIS..Forecast_2026_UnionAll_20260227'
										
										
--================================ PART 1 - CALCULATION ============================									

----------================== CREATE TABLE CALCULATION----------------------------------									

		IF EXISTS ( SELECT * FROM ANALYSIS.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'FORECAST_2026_CALC' )
		DROP TABLE ANALYSIS..FORECAST_2026_CALC					
									
		CREATE TABLE ANALYSIS..FORECAST_2026_CALC	
		(BIZLINE			VARCHAR(20)
		,YEARMONTH			VARCHAR(20)									
		,TYPE				VARCHAR(40)  ------Non_earn cus/BUDGET/FORECAST/FORECAST_LM									
		,CUS_TYPE			VARCHAR(30)								
		,SEGMENT			VARCHAR(10)		
		,PRODUCT_LV_4		VARCHAR(90)	 -----TOTAL LENDING/CARD , TOTAL DEPOSIT , FEE BASE ....							
		,PRODUCT_LV_3		VARCHAR(90)	 -----PRODUCT_FAMILY  		(LENDING,CARD ...)						
		,PRODUCT_LV_2		VARCHAR(90)	 -----PRODUCT_LINE			(MORTGAGE ...)							
		,PRODUCT_LV_1		VARCHAR(90)	 -----SUB_PRODUCT			(MORTGAGE PRIMARY...)							
		,MEASURE			VARCHAR(90)							
		,LEVEL				VARCHAR(10)							
		,VALUE				FLOAT
		)									

----------================== A. CONSOL RAW DATA FOR BALANCE + TOI: Non_earn cus + BUDGET + FORECAST ------------------------------------------------									

EXEC('
									
		IF EXISTS ( SELECT * FROM TEMPDB.INFORMATION_SCHEMA.TABLES	WHERE TABLE_NAME = ''##FORECAST_CONSOL'' )
		DROP TABLE ##FORECAST_CONSOL

		SELECT * INTO ##FORECAST_CONSOL
		FROM( 
			SELECT BIZLINE, MONTH, ''BUDGET'' AS TYPE, CUS_TYPE, SEGMENT, '''' SUB_CHANNEL, CHANNEL ,PRODUCT_FAMILY, PRODUCT_LINE, SUB_PRODUCT, GROUP_MEASURE, MEASURE, SUB_MEASURE, SUB_MEASURE_2, VALUE
			FROM '+@TBL_BUDGET_2026+'
			WHERE MONTH LIKE ''2026%''

			UNION ALL
			SELECT BIZLINE, MONTH,''FORECAST'' AS TYPE, CUS_TYPE, SEGMENT, SUB_CHANNEL, CHANNEL, PRODUCT_FAMILY, PRODUCT_LINE, SUB_PRODUCT, GROUP_MEASURE, MEASURE, SUB_MEASURE,SUB_MEASURE_2, VALUE
			FROM '+@TBL_FORECAST_2026+'	
			
			UNION ALL 
			SELECT BIZLINE, MONTH,''FORECAST_LM'' AS TYPE, CUS_TYPE, SEGMENT, '''' SUB_CHANNEL, CHANNEL, PRODUCT_FAMILY, PRODUCT_LINE, SUB_PRODUCT, GROUP_MEASURE, MEASURE, SUB_MEASURE,SUB_MEASURE_2, VALUE
			FROM '+@TBL_FORECAST_LM+' where MONTH LIKE ''2026%''

			UNION ALL
			SELECT BIZLINE, MONTH, ''REVISED_BUDGET'' AS TYPE, CUS_TYPE, SEGMENT, '''' SUB_CHANNEL, CHANNEL ,PRODUCT_FAMILY, PRODUCT_LINE, SUB_PRODUCT, GROUP_MEASURE, MEASURE, SUB_MEASURE,SUB_MEASURE_2, VALUE
			FROM ANALYSIS..REVISED_BUDGET_2026_UNIONALL
			WHERE MONTH LIKE ''2026%''
			 ) A
	')

	UPDATE  ##FORECAST_CONSOL                           --- Update 20251125
	SET  SUB_MEASURE = 'CI', SUB_MEASURE_2 = 'CI' 
	WHERE SUB_MEASURE = 'Other NII' AND BIZLINE = 'SME'

	UPDATE  ##FORECAST_CONSOL                            --- Update 20251125
	SET  SUB_MEASURE = 'FI' 
	WHERE SUB_MEASURE = 'NFI' AND BIZLINE = 'RETAIL' AND PRODUCT_LINE = 'CASA&AE'


----------------------------------L1 - 1.1. DETAILS SUB_MEASURE OF NII, NFI: CI,FTP,INT_REV,CI_ADJ, FI,FE ----------------------------------									

		INSERT INTO ANALYSIS..FORECAST_2026_CALC									
		SELECT   BIZLINE,MONTH									
				,TYPE	
				,CUS_TYPE,SEGMENT
				,CASE WHEN PRODUCT_FAMILY IN ('CARD','LENDING') THEN 'TOTAL LENDING/CARD'							
					  WHEN PRODUCT_FAMILY IN ('TIME DEPOSIT','CASA', 'DEPOSIT','CASA&AE') THEN 'TOTAL DEPOSIT'						
					  ELSE PRODUCT_FAMILY END						
				,PRODUCT_FAMILY					
				,PRODUCT_LINE						
				,SUB_PRODUCT 						
				,CASE WHEN GROUP_MEASURE IN ('NFI') AND SUB_MEASURE NOT IN ('FI','FE') THEN 'FI' ELSE SUB_MEASURE END	
				,'L1' LEVEL							
				,SUM(VALUE) VALUE							
		FROM ##FORECAST_CONSOL								
		WHERE GROUP_MEASURE IN ('NII','NFI') AND SUB_MEASURE <> GROUP_MEASURE									
		GROUP BY BIZLINE,MONTH									
				,TYPE	
				,CUS_TYPE,SEGMENT
				,CASE WHEN PRODUCT_FAMILY IN ('CARD','LENDING') THEN 'TOTAL LENDING/CARD'							
					  WHEN PRODUCT_FAMILY IN ('TIME DEPOSIT','CASA', 'DEPOSIT','CASA&AE') THEN 'TOTAL DEPOSIT'						
					  ELSE PRODUCT_FAMILY END						
				,PRODUCT_FAMILY					
				,PRODUCT_LINE						
				,SUB_PRODUCT 						
				,CASE WHEN GROUP_MEASURE IN ('NFI') AND SUB_MEASURE NOT IN ('FI','FE') THEN 'FI' ELSE SUB_MEASURE END


----------------------------------L1 - 1.1.2. DETAILS SUB_MEASURE_2 OF CD ----------------------------------									
										
		INSERT INTO ANALYSIS..FORECAST_2026_CALC 									
		SELECT   BIZLINE,MONTH									
				,TYPE	
				,CUS_TYPE,SEGMENT
				,CASE WHEN PRODUCT_FAMILY IN ('CARD','LENDING') THEN 'TOTAL LENDING/CARD'							
					  WHEN PRODUCT_FAMILY IN ('TIME DEPOSIT','CASA', 'DEPOSIT') THEN 'TOTAL DEPOSIT'						
					  ELSE PRODUCT_FAMILY END						
				,PRODUCT_FAMILY					
				,PRODUCT_LINE						
				,SUB_PRODUCT 						
				,CASE WHEN SUB_MEASURE_2 IN ('CI') THEN 'CI_Normal' ELSE SUB_MEASURE_2 END	
				,'L1' LEVEL							
				,SUM(VALUE) VALUE							
		FROM ##FORECAST_CONSOL								
		WHERE GROUP_MEASURE IN ('NII') AND PRODUCT_LINE IN ('CD','CD Self Trade', 'CDST', 'CD AE')  AND SUB_MEASURE_2 IN ('CI','LIQUIDITY COST') -- update: 20251212								
		GROUP BY BIZLINE,MONTH									
				,TYPE	
				,CUS_TYPE,SEGMENT
				,CASE WHEN PRODUCT_FAMILY IN ('CARD','LENDING') THEN 'TOTAL LENDING/CARD'							
					  WHEN PRODUCT_FAMILY IN ('TIME DEPOSIT','CASA', 'DEPOSIT') THEN 'TOTAL DEPOSIT'						
					  ELSE PRODUCT_FAMILY END						
				,PRODUCT_FAMILY					
				,PRODUCT_LINE						
				,SUB_PRODUCT 						
				,CASE WHEN SUB_MEASURE_2 IN ('CI') THEN 'CI_Normal' ELSE SUB_MEASURE_2 END	

----------------------------------L1 - 1.2. DETAILS MEASURE: .'NII','AVG BAL','EOP BAL','NEW BAL','PAIDOFF','NEW CARD','IB BAL','OUT BAL','APE','NEW ACCT'----------------------------------						
			
		INSERT INTO ANALYSIS..FORECAST_2026_CALC									
		SELECT   BIZLINE,MONTH									
				,TYPE	
				,CUS_TYPE,SEGMENT
				,CASE WHEN PRODUCT_FAMILY IN ('CARD','LENDING')		 THEN 'TOTAL LENDING/CARD'							
					  WHEN PRODUCT_FAMILY IN ('TIME DEPOSIT','CASA', 'DEPOSIT','CASA&AE') THEN 'TOTAL DEPOSIT'						
					  ELSE PRODUCT_FAMILY END						
				,PRODUCT_FAMILY					
				,PRODUCT_LINE						
				,SUB_PRODUCT 						
				,CASE WHEN GROUP_MEASURE IN ('PAIDOFF','NII','NFI')		 THEN GROUP_MEASURE  
					  WHEN SUB_MEASURE IN ('Purchase Amt','Purchase No') THEN SUB_MEASURE  
					  ELSE MEASURE END			
				,'L1' LEVEL							
				,SUM(VALUE)  VALUE							
		FROM ##FORECAST_CONSOL					
		WHERE (MEASURE IN (									
						  'NII','NFI'
						 ,'AVG BAL','EOP BAL','NEW BAL'					
						 ,'IB BAL','OUT BAL'					
						 ,'PAIDOFF'					
						 ,'NEW CARD'					
						 ,'APE','FYP','GWP','NEW ACCT'					
						 ,'LIQUIDITY RECHARGED','TRADING INCOME','OTHER INCOME'			
						 ,'CUS HAVING NEW CONTRACT'	, 'REGISTER CUS'	
						 ,'UNIQUE CUS HAVING NEW CONTRACT'
						 ,'NEW CUS'
						 ,'EOP Cus by product'
						 ,'FYP'
						 ,'EOP CARD','ACTIVE CARD'
						 ,'OPEX','Provision'
						 ,'Limit Approved', 'Limit Used', 'Cus having New Bal', 'EOP Cus having limit', 'Limit'
						 ,'Margin point', 'Ticket size FX'
						 )		
				OR 	GROUP_MEASURE IN ('PAIDOFF','NII','NFI')	
				OR 	(	MEASURE IN ('TNX No','TNX Amt', 'TXN Amt', 'TXN No', 'TTN')	
					 AND	(PRODUCT_FAMILY IN ('CASA', 'Fee base', 'CASA&AE') 
					 OR SUB_MEASURE IN ('Purchase Amt','Purchase No')) ) 
					 ) and product_line <> 'Other asset'   -- update 20260226
		GROUP BY BIZLINE,MONTH									
				,TYPE	
				,CUS_TYPE,SEGMENT
				,CASE WHEN PRODUCT_FAMILY IN ('CARD','LENDING')		 THEN 'TOTAL LENDING/CARD'							
					  WHEN PRODUCT_FAMILY IN ('TIME DEPOSIT','CASA', 'DEPOSIT','CASA&AE') THEN 'TOTAL DEPOSIT'						
					  ELSE PRODUCT_FAMILY END						
				,PRODUCT_FAMILY					
				,PRODUCT_LINE						
				,SUB_PRODUCT 						
				,CASE WHEN GROUP_MEASURE IN ('PAIDOFF','NII','NFI')		 THEN GROUP_MEASURE  
					  WHEN SUB_MEASURE IN ('Purchase Amt','Purchase No') THEN SUB_MEASURE  
					  ELSE MEASURE END			


---------------------------------- 1.3.'EOP CUS','EOP CUS HOLDING','EOP CUS ACTIVE'	----------------------------------									

		INSERT INTO ANALYSIS..FORECAST_2026_CALC									
		SELECT A.BIZLINE,A.MONTH,A.TYPE,A.CUS_TYPE,A.SEGMENT	
			  ,A.PRODUCT_LV_4,A.PRODUCT_LV_3,A.PRODUCT_LV_2,A.PRODUCT_LV_1,A.MEASURE
			  ,LEVEL,VALUE
		FROM
		(
		SELECT   BIZLINE, MONTH									
				,TYPE						
				,CUS_TYPE,SEGMENT							
				,CASE WHEN PRODUCT_FAMILY IN ('CARD','LENDING')		 THEN 'TOTAL LENDING/CARD'							
					  WHEN PRODUCT_FAMILY IN ('TIME DEPOSIT','CASA','CASA&AE') THEN 'TOTAL DEPOSIT'						
					  ELSE PRODUCT_FAMILY END	AS PRODUCT_LV_4				
				,PRODUCT_FAMILY					AS PRODUCT_LV_3
				,CASE WHEN PRODUCT_FAMILY = 'CASA'	THEN 'ALL' 
					  ELSE PRODUCT_LINE END		AS PRODUCT_LV_2		
				,CASE WHEN PRODUCT_FAMILY in ('CASA','CASA&AE', 'TIME DEPOSIT')	THEN 'ALL'
					  WHEN PRODUCT_FAMILY IN ('CARD','LENDING') and BIZLINE = 'SME'	THEN 'ALL' 
					  WHEN PRODUCT_FAMILY IN ('CARD') AND BIZLINE = 'Retail' then 'All'
					  WHEN PRODUCT_LINE IN ('Bond-Fund', 'Structured Notes') AND BIZLINE = 'Retail' then 'All'
					  ELSE SUB_PRODUCT END		AS PRODUCT_LV_1			
				,MEASURE		
				,SUM(VALUE) VALUE							
		FROM ##FORECAST_CONSOL					
		WHERE MEASURE IN ( 'EOP CUS','EOP CUS HOLDING','EOP CUS ACTIVE', 'ACTIVE CUS')	OR MEASURE IN ('Truly Active Cus','Low Active Cus','MTB Cus')
		GROUP BY BIZLINE, MONTH									
				,TYPE						
				,CUS_TYPE,SEGMENT							
				,CASE WHEN PRODUCT_FAMILY IN ('CARD','LENDING')		 THEN 'TOTAL LENDING/CARD'							
					  WHEN PRODUCT_FAMILY IN ('TIME DEPOSIT','CASA','CASA&AE') THEN 'TOTAL DEPOSIT'						
					  ELSE PRODUCT_FAMILY END					
				,PRODUCT_FAMILY						
				,CASE WHEN PRODUCT_FAMILY = 'CASA'	THEN 'ALL' 
					  ELSE PRODUCT_LINE END				
				,CASE WHEN PRODUCT_FAMILY in ('CASA','CASA&AE', 'TIME DEPOSIT')	THEN 'ALL'
					  WHEN PRODUCT_FAMILY IN ('CARD','LENDING') and BIZLINE = 'SME'	THEN 'ALL' 
					  WHEN PRODUCT_FAMILY IN ('CARD') AND BIZLINE = 'Retail' then 'All'
					   WHEN PRODUCT_LINE IN ('Bond-Fund', 'Structured Notes') AND BIZLINE = 'Retail' then 'All'
					  ELSE SUB_PRODUCT END					
				,MEASURE 	

		) A
		LEFT JOIN  ANALYSIS..REVISED_BUDGET_2026_X B
		ON   A.PRODUCT_LV_4 = B.PRODUCT_LV_4
		AND  A.PRODUCT_LV_3 = B.PRODUCT_LV_3
		AND  A.PRODUCT_LV_2 = B.PRODUCT_LV_2
		AND  A.PRODUCT_LV_1 = B.PRODUCT_LV_1
		AND  A.MEASURE		= B.MEASURE
		AND  A.BIZLINE		= B.BIZLINE

----------------------------------L0. 'NEW CUS' BY CUS_GROUP----------------------------------						
			
		INSERT INTO ANALYSIS..FORECAST_2026_CALC									
		SELECT   BIZLINE,MONTH									
				,TYPE	
				,CUS_TYPE,SEGMENT
				,PRODUCT_FAMILY						
				,PRODUCT_FAMILY					
				,PRODUCT_LINE						
				,SUB_PRODUCT 						
				,CHANNEL AS MEASURE
				,'L0' LEVEL							
				,SUM(VALUE)  VALUE							
		FROM ##FORECAST_CONSOL					
		WHERE MEASURE IN ('NEW CUS' )	AND PRODUCT_FAMILY ='CUS'	
		GROUP BY BIZLINE,MONTH									
				,TYPE	
				,CUS_TYPE,SEGMENT
				,PRODUCT_FAMILY					
				,PRODUCT_FAMILY					
				,PRODUCT_LINE						
				,SUB_PRODUCT 						
				,CHANNEL

----------------------------------L0.1 'NEW CUS' BY SUB-CHANNEL----------------------------------						
			
		INSERT INTO ANALYSIS..FORECAST_2026_CALC 									
		SELECT   BIZLINE, MONTH									
				,TYPE	
				,CUS_TYPE,SEGMENT
				,PRODUCT_FAMILY						
				,PRODUCT_FAMILY					
				,PRODUCT_LINE						
				,SUB_PRODUCT 						
				,SUB_CHANNEL AS MEASURE
				,'L0' LEVEL							
				,SUM(VALUE)  VALUE							
		FROM ##FORECAST_CONSOL					
		WHERE BIZLINE = 'RETAIL'  AND MEASURE IN ('NEW CUS' )	AND PRODUCT_FAMILY ='CUS'	
		GROUP BY BIZLINE, MONTH									
				,TYPE	
				,CUS_TYPE,SEGMENT
				,PRODUCT_FAMILY					
				,PRODUCT_FAMILY					
				,PRODUCT_LINE						
				,SUB_PRODUCT 						
				,SUB_CHANNEL

----------------------------------L0.2 'NEW CUS' CASA&AE----------------------------------	
		INSERT INTO ANALYSIS..FORECAST_2026_CALC 									
		SELECT   BIZLINE, MONTH									
				,TYPE	
				,CUS_TYPE,SEGMENT
				,'Total Deposit' PRODUCT_LV_4						
				,PRODUCT_FAMILY					
				,PRODUCT_LINE						
				,'All' 						
				,'New Cus CASA&AE' MEASURE
				,'L2' LEVEL							
				,SUM(VALUE)  VALUE							
		FROM ##FORECAST_CONSOL					
		WHERE BIZLINE = 'RETAIL'  AND MEASURE IN ('EOP CUS' )	AND CUS_TYPE = 'New' AND  SUB_PRODUCT ='CASA&AE'	
		GROUP BY BIZLINE, MONTH									
				,TYPE	
				,CUS_TYPE,SEGMENT
				,PRODUCT_FAMILY					
				,PRODUCT_FAMILY					
				,PRODUCT_LINE	


----------------- 20251125: Update FI, FE ----------------------------------		
		INSERT INTO ANALYSIS..FORECAST_2026_CALC    
		SELECT BIZLINE, MONTH, TYPE
			  ,' ALL' CUS_TYPE, ' ALL' SEGMENT 
			  , CASE WHEN PRODUCT_LINE IN ('CASA','CASA&AE') OR SUB_PRODUCT IN ('Home Banking','Remittance', 'Echannel','MQR','Soundbox','POS' ) THEN 'CASA Group'
					 WHEN product_family = 'Time Deposit' THEN 'Total Deposit'
					 WHEN PRODUCT_FAMILY IN ('LENDING') OR PRODUCT_LINE = 'Credit Card' THEN 'Total Lending/Card'
					 WHEN PRODUCT_LINE = 'Debit card' then 'CASA Group'
				ELSE PRODUCT_FAMILY END AS PRODUCT_LV_4
			  , CASE WHEN SUB_PRODUCT IN ('POS','MQR','Soundbox') and bizline = 'Retail' THEN 'CASA&AE' 
					 WHEN PRODUCT_LINE = 'Debit Card' THEN 'Card'
					 --WHEN PRODUCT_LINE = 'CASA' THEN 'CASA&AE' 
					 --WHEN PRODUCT_LINE = 'CA' THEN 'CASA'
			    ELSE PRODUCT_LINE END AS PRODUCT_LV_3
			  , CASE WHEN PRODUCT_LINE = 'TD Normal' THEN 'All' 
					 WHEN SUB_PRODUCT IN ('Home Banking','Remittance', 'Echannel','POS','MQR','Soundbox') THEN SUB_PRODUCT 
				ELSE PRODUCT_LINE END AS PRODUCT_LV_2  
			  , 'All'  PRODUCT_LV_1
			  ,  SUB_MEASURE MEASURE
			  , 'L1.5' LEVEL
			  , VALUE

		from ##FORECAST_CONSOL
		where 1=1 AND 
	   (BIZLINE = 'RETAIL'
		and  sub_measure in ('FI','FE')
		and  product_line in ('Credit Card','Debit card','CASA','Bond-Fund', 'Structured Notes','Home Banking','Remittance', 'Echannel','ATM','Other fee','CASA&AE','TD Normal')
		) OR 
		(BIZLINE = 'SME'
		and  sub_measure in ('FI','FE')
		and  product_line in ('Credit Card','Debit card','CASA','CA')
		) --Update 20260226



----------================== B. CONSOL RAW DATA FOR OPEX + PROVISION ------------------------------------------------									

----------================== C. SUM-UP CALCULATION ------------------------------------------------									
	
---------------------------------- L2 SUM-UP CALCULATION update 20260226----------------------------------	

		INSERT INTO ANALYSIS..FORECAST_2026_CALC									
		SELECT  BIZLINE, YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,CASE WHEN PRODUCT_LV_4 = 'CASA&AE' AND MEASURE IN ('CI','FTP') THEN 'TOTAL DEPOSIT' 
				 ELSE PRODUCT_LV_4 END AS PRODUCT_LV_4
				 ,PRODUCT_LV_3
				,PRODUCT_LV_2
				,'ALL'			
				,MEASURE				
				,'L2' LEVEL							
				,SUM(VALUE) VALUE							
		FROM ANALYSIS..FORECAST_2026_CALC 									
		WHERE LEVEL ='L1'									
			  AND ( MEASURE NOT IN ( 'EOP CUS','EOP CUS HOLDING','EOP CUS ACTIVE' )	 )    							
		GROUP BY BIZLINE,YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,CASE WHEN PRODUCT_LV_4 = 'CASA&AE' AND MEASURE IN ('CI','FTP') THEN 'TOTAL DEPOSIT' 
				 ELSE PRODUCT_LV_4 END 		
				,PRODUCT_LV_3
				,PRODUCT_LV_2
				,MEASURE	
								

---------------------------------- L2.5 CASA GROUP (NFI) SUM-UP CALCULATION ----------------------------------	
		INSERT INTO ANALYSIS..FORECAST_2026_CALC 									  ---update 20260226		
		SELECT  BIZLINE, YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,PRODUCT_LV_4
				,PRODUCT_LV_3
				, PRODUCT_LV_2			
				,'ALL'	PRODUCT_LV_1		
				, MEASURE							
				,'L2.5' LEVEL							
				,SUM(VALUE) VALUE							
		FROM ANALYSIS..FORECAST_2026_CALC 		 									
		WHERE LEVEL ='L1.5'									
			  AND MEASURE IN ('FI','FE'  )	
			  AND PRODUCT_LV_2  IN ('CASA&AE','HOME BANKING','ECHANNEL','REMITTANCE', 'CASA', 'DEBIT CARD','POS','MQR','Soundbox')			
		GROUP BY BIZLINE, YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,PRODUCT_LV_4
				,PRODUCT_LV_3 
				,PRODUCT_LV_2
				,MEASURE

		INSERT INTO ANALYSIS..FORECAST_2026_CALC 											
		SELECT  BIZLINE, YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,'CASA GROUP'		
				,'CASA GROUP'		
				,'ALL'			
				,'ALL'			
				,MEASURE							
				,'L3.5' LEVEL							
				,SUM(VALUE) VALUE							
		FROM ANALYSIS..FORECAST_2026_CALC 											
		WHERE LEVEL ='L2.5'									
			  AND MEASURE IN ('FI','FE')	
			  AND PRODUCT_LV_2  IN ('CASA&AE','CASA', 'HOME BANKING','ECHANNEL','REMITTANCE','DEBIT CARD','POS','MQR','Soundbox','CA')	
		GROUP BY BIZLINE, YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,MEASURE		

		INSERT INTO ANALYSIS..FORECAST_2026_CALC 		
		SELECT  BIZLINE, YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,PRODUCT_LV_4
				,CASE WHEN PRODUCT_LV_3 = 'CASA' AND BIZLINE = 'Retail' THEN 'CASA&AE' 
				 ELSE PRODUCT_LV_3 END AS PRODUCT_LV_3
				, PRODUCT_LV_2			
				,'ALL'			
				, 'NFI' MEASURE							
				,'L2.5' LEVEL							
				,SUM(VALUE) VALUE							
		FROM ANALYSIS..FORECAST_2026_CALC 									
		WHERE LEVEL ='L1.5'									
			  AND MEASURE IN ('FI','FE'  )	
			  AND PRODUCT_LV_2  IN ('CASA&AE','HOME BANKING','ECHANNEL','REMITTANCE', 'CASA', 'DEBIT CARD','POS','MQR','Soundbox')			
		GROUP BY BIZLINE, YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,PRODUCT_LV_4
				,CASE WHEN PRODUCT_LV_3 = 'CASA' AND BIZLINE = 'Retail' THEN 'CASA&AE' 
				 ELSE PRODUCT_LV_3 END
				, PRODUCT_LV_2
				, MEASURE

		INSERT INTO ANALYSIS..FORECAST_2026_CALC 					--Update 20260226					
		SELECT  BIZLINE, YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,'CASA GROUP'		
				,'CASA GROUP'		
				,'ALL'			
				,'ALL'			
				,'NFI' MEASURE							
				,'L3.5' LEVEL							
				,SUM(VALUE) VALUE							
		FROM ANALYSIS..FORECAST_2026_CALC 											
		WHERE LEVEL ='L1.5'									
			  AND MEASURE IN ('FI','FE')	
			  AND PRODUCT_LV_2  IN ('CASA&AE','CASA', 'HOME BANKING','ECHANNEL','REMITTANCE','DEBIT CARD','POS','MQR','Soundbox','CA')	
		GROUP BY BIZLINE, YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				

		---- 20251125: Update TRADE SERVICE & FX FLOW
		INSERT INTO ANALYSIS..FORECAST_2026_CALC 		
		SELECT BIZLINE,YEARMONTH									
				,TYPE							
				,' All'CUS_TYPE,' All'SEGMENT
				,'Fee base'		
				,'Fee base'		
				,CASE WHEN PRODUCT_LV_1 IN ('LC Fee','LG Fee') AND MEASURE = 'NFI' THEN 'Trade Service Fee'              
					  WHEN PRODUCT_LV_1 IN ('LC Interest','LG Interest') AND MEASURE = 'NII' THEN 'Trade Service Interest'
					  WHEN PRODUCT_LV_1  IN ('FX Flow','FX Derivatives')  THEN 'FX Flow'
					  END	
				,'ALL'			
				,MEASURE							
				,'L3.5' LEVEL							
				,SUM(VALUE) VALUE							
		FROM ANALYSIS..FORECAST_2026_CALC 			 									
		WHERE LEVEL ='L1'	AND BIZLINE = 'SME'								
			  AND MEASURE IN ( 'NFI', 'NII','Trading Income')	
			  AND (  PRODUCT_LV_2  IN ('LC','LG') OR PRODUCT_LV_1  IN ('FX Flow','FX Derivatives') )		
		GROUP BY BIZLINE,YEARMONTH									
				,TYPE							
				,CASE WHEN PRODUCT_LV_1 IN ('LC Fee','LG Fee') AND MEASURE = 'NFI' THEN 'Trade Service Fee'              
					  WHEN PRODUCT_LV_1 IN ('LC Interest','LG Interest') AND MEASURE = 'NII' THEN 'Trade Service Interest'
					  WHEN PRODUCT_LV_1  IN ('FX Flow','FX Derivatives')  THEN 'FX Flow'
					  END
				,MEASURE	
				
---------------------------------- L3 SUM-UP CALCULATION ----------------------------------	

		INSERT INTO ANALYSIS..FORECAST_2026_CALC									
		SELECT  BIZLINE, YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,PRODUCT_LV_4		
				,PRODUCT_LV_3
				,CASE WHEN PRODUCT_LV_2 IN ('Bond-Fund', 'Structured Notes') THEN 'Investment Product' ELSE 'ALL' END AS PRODUCT_LV_2			
				,'ALL'			
				,MEASURE							
				,'L3' LEVEL							
				,SUM(VALUE) VALUE							
		FROM ANALYSIS..FORECAST_2026_CALC									
		WHERE LEVEL ='L2'									
			  AND MEASURE NOT IN ( 'EOP CUS','EOP CUS HOLDING','EOP CUS ACTIVE')								
		GROUP BY BIZLINE, YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,PRODUCT_LV_4		
				,PRODUCT_LV_3
				,CASE WHEN PRODUCT_LV_2 IN ('Bond-Fund', 'Structured Notes') THEN 'Investment Product' ELSE 'ALL' END
				,MEASURE							


---------------------------------- L4 SUM-UP CALCULATION ----------------------------------	

		INSERT INTO ANALYSIS..FORECAST_2026_CALC									
		SELECT   BIZLINE,YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,PRODUCT_LV_4		
				,'ALL'			
				,'ALL'			
				,'ALL'			
				,MEASURE							
				,'L4' LEVEL							
				,SUM(VALUE) VALUE							
		FROM ANALYSIS..FORECAST_2026_CALC									
		WHERE LEVEL ='L3'									
			  AND ( MEASURE NOT IN ( 'EOP CUS','EOP CUS HOLDING','EOP CUS ACTIVE'  ))	   
		GROUP BY BIZLINE,YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,PRODUCT_LV_4		
				,MEASURE							
							

---------------------------------- L5 SUM-UP CALCULATION ----------------------------------	

----------------------------------  5.1 'NII','NFI','TRADING INCOME','OTHER INCOME','PROVISION','OPEX','FI','FE' ALL PRODUCT---------------------------------									
		INSERT INTO ANALYSIS..FORECAST_2026_CALC									
		SELECT   BIZLINE,YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,'ALL'			
				,'ALL'			
				,'ALL'			
				,'ALL'			
				,MEASURE							
				,'L5' LEVEL							
				,SUM(VALUE) VALUE							
		FROM ANALYSIS..FORECAST_2026_CALC									
		WHERE LEVEL='L4'									
			  AND MEASURE IN ('NII','NFI','TRADING INCOME','OTHER INCOME','PROVISION','OPEX','FI','FE')								
		GROUP BY BIZLINE,YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,MEASURE							
									

----------------------------------  5.2 'TOI' ALL PRODUCT---------------------------------									
		INSERT INTO ANALYSIS..FORECAST_2026_CALC	
		SELECT  BIZLINE, YEARMONTH									
				,TYPE							
				,' ALL', ' ALL'
				,'ALL'			
				,'ALL'			
				,'ALL'			
				,'ALL'			
				,MEASURE						
				,'L4.5' LEVEL							
				,SUM(VALUE) VALUE							
		FROM ANALYSIS..FORECAST_2026_CALC									
		WHERE 1=1 
		    and ( MEASURE IN ('FI','FE') AND ((PRODUCT_LV_3 = 'CASA Group' and Level = 'L3.5') 
		                                 OR (PRODUCT_LV_2 IN ('Bond-Fund', 'Structured Notes') and PRODUCT_LV_1 = 'All' AND LEVEL = 'L2')
										 OR (PRODUCT_LV_2 IN ( 'ATM','Merchant Payment Collection', 'Other fee') and LEVEL = 'L1.5')
										 OR (PRODUCT_LV_3 = 'Time Deposit' AND level = 'L1.5')
										 OR (PRODUCT_LV_2 = 'Credit Card' AND PRODUCT_LV_1 = 'All' and level = 'L1.5')
										 ))
			OR (MEASURE = 'FI' AND PRODUCT_LV_2 = 'Banca' and PRODUCT_LV_1 IN ('Banca life','Banca Non_Life') and level = 'L1')
		GROUP BY BIZLINE, YEARMONTH									
				,TYPE							
				, MEASURE
		
		INSERT INTO ANALYSIS..FORECAST_2026_CALC	 
		SELECT  BIZLINE, YEARMONTH									
				,TYPE							
				,' ALL',' ALL'
				,'ALL'			
				,'ALL'			
				,'ALL'			
				,'ALL'			
				,'NFI'						
				,'L4.5' LEVEL							
				,SUM(VALUE) VALUE							
		FROM ANALYSIS..FORECAST_2026_CALC	 									
		WHERE 1=1 
			and ( MEASURE IN ('FI','FE') AND ((PRODUCT_LV_3 = 'CASA Group' and Level = 'L3.5') 
		                                 OR (PRODUCT_LV_2 IN ('Bond-Fund', 'Structured Notes') AND LEVEL = 'L2')
										 OR (PRODUCT_LV_2 IN ('Credit Card', 'ATM','Merchant Payment Collection', 'Other fee') and LEVEL = 'L1.5')
										 OR (PRODUCT_LV_3 = 'Time Deposit' AND level = 'L1.5')
										 )
			or MEASURE = 'NFI' AND PRODUCT_LV_2 IN ('Banca','DPM', 'GOLD') and level = 'L1'
			   )
		GROUP BY BIZLINE, YEARMONTH									
				,TYPE							

		INSERT INTO ANALYSIS..FORECAST_2026_CALC	 									
		SELECT  BIZLINE, YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,'ALL'			
				,'ALL'			
				,'ALL'			
				,'ALL'			
				,'TOI'							
				,'L5' LEVEL							
				,SUM(VALUE) VALUE							
		FROM ANALYSIS..FORECAST_2026_CALC	 									
		WHERE LEVEL='L4'									
			  AND MEASURE IN ('NII','NFI','TRADING INCOME','OTHER INCOME' )								
		GROUP BY BIZLINE, YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
						

----------------------------------  5.4 'AUM' ALL PRODUCT---------------------------------									
		INSERT INTO ANALYSIS..FORECAST_2026_CALC	 		
		SELECT BIZLINE, YEARMONTH, TYPE, CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL, SUM(VALUE) VALUE
		FROM (
		SELECT  BIZLINE, YEARMONTH									
				,TYPE							
				,CUS_TYPE, SEGMENT
				,'ALL'		PRODUCT_LV_4	
				,'ALL'		PRODUCT_LV_3	
				,'ALL'		PRODUCT_LV_2	
				,'ALL'		PRODUCT_LV_1	
				,'AUM'		MEASURE					
				,'L5' LEVEL							
				,SUM(VALUE) VALUE							
		FROM ANALYSIS..FORECAST_2026_CALC	 										
		WHERE LEVEL='L1'									
			  AND MEASURE IN ('EOP BAL' )	AND (PRODUCT_LV_3 IN ('CASA','Time Deposit', 'CASA&AE', 'DPM') OR PRODUCT_LV_2 in ('Bond-Fund','Structured Notes'))  -- Update 20250925 
		GROUP BY BIZLINE, YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT

	    UNION ALL 
									
		SELECT  BIZLINE, MONTH , TYPE							
				, CUS_TYPE, SEGMENT
				,'ALL'	PRODUCT_LV_4		
				,'ALL'	PRODUCT_LV_3		
				,'ALL'	PRODUCT_LV_2		
				,'ALL'	PRODUCT_LV_1		
				,'AUM'	MEASURE						
				,'L5' LEVEL							
				,SUM(VALUE) VALUE							
		FROM ##FORECAST_CONSOL									
		WHERE PRODUCT_LINE = 'Other Asset' AND MEASURE = 'EOP BAL'
		GROUP BY BIZLINE, MONTH	,TYPE, CUS_TYPE, SEGMENT							
				) A
		GROUP BY BIZLINE, YEARMONTH, TYPE, CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL


---------------------------------- CUS_TYPE  ALL ----------------------------------									
		INSERT INTO ANALYSIS..FORECAST_2026_CALC									
		SELECT   BIZLINE,YEARMONTH									
				,TYPE							
				,' ALL'
				,SEGMENT
				,PRODUCT_LV_4		
				,PRODUCT_LV_3				
				,PRODUCT_LV_2
				,PRODUCT_LV_1			
				,MEASURE							
				,LEVEL							
				,SUM(VALUE) VALUE							
		FROM ANALYSIS..FORECAST_2026_CALC
		WHERE  LEVEL NOT IN ( 'L1.5', 'L2.5','L3.5', 'L4.5') 
		GROUP BY BIZLINE,YEARMONTH									
				,TYPE							
				,SEGMENT
				,PRODUCT_LV_4		
				,PRODUCT_LV_3				
				,PRODUCT_LV_2
				,PRODUCT_LV_1			
				,MEASURE							
				,LEVEL							

---------------------------------- SEGMENT  ALL ----------------------------------									
		INSERT INTO ANALYSIS..FORECAST_2026_CALC									
		SELECT   BIZLINE,YEARMONTH									
				,TYPE
				,CUS_TYPE						
				,' ALL'
				,PRODUCT_LV_4		
				,PRODUCT_LV_3				
				,PRODUCT_LV_2
				,PRODUCT_LV_1			
				,MEASURE							
				,LEVEL							
				,SUM(VALUE) VALUE							
		FROM ANALYSIS..FORECAST_2026_CALC
		WHERE LEVEL NOT IN  ('L1.5','L2.5','L3.5', 'L4.5') 
		GROUP BY BIZLINE,YEARMONTH									
				,TYPE							
				,CUS_TYPE	
				,PRODUCT_LV_4		
				,PRODUCT_LV_3				
				,PRODUCT_LV_2
				,PRODUCT_LV_1			
				,MEASURE							
				,LEVEL							

---------------------------------- BIZLINE  ALL ----------------------------------									
		INSERT INTO ANALYSIS..FORECAST_2026_CALC 									
		SELECT   'ALL',YEARMONTH									
				,TYPE
				,CUS_TYPE						
				,SEGMENT
				,PRODUCT_LV_4		
				,PRODUCT_LV_3				
				,PRODUCT_LV_2
				,PRODUCT_LV_1			
				,MEASURE							
				,LEVEL							
				,SUM(VALUE) VALUE							
		FROM ANALYSIS..FORECAST_2026_CALC 		
		WHERE LEVEL NOT IN  ('L1.5', 'L2.5','L3.5', 'L4.5')
		GROUP BY YEARMONTH									
				,TYPE							
				,CUS_TYPE
				,SEGMENT
				,PRODUCT_LV_4		
				,PRODUCT_LV_3				
				,PRODUCT_LV_2
				,PRODUCT_LV_1			
				,MEASURE							
				,LEVEL		


---------------------------------- AVG BAL &  EOP CUS_SUM ---------------------------------									
		INSERT INTO ANALYSIS..FORECAST_2026_CALC 									
		SELECT   BIZLINE,YEARMONTH									
				,TYPE
				,CUS_TYPE						
				,SEGMENT
				,PRODUCT_LV_4		
				,PRODUCT_LV_3				
				,PRODUCT_LV_2
				,PRODUCT_LV_1			
				,MEASURE+'_SUM'							
				,LEVEL							
				,SUM(VALUE) VALUE							
		FROM ANALYSIS..FORECAST_2026_CALC 
		WHERE (MEASURE IN ( 'EOP CUS', 'ACTIVE CUS') AND PRODUCT_LV_3 = 'CASA&AE')    ----------- 20251125: Update
		GROUP BY BIZLINE,YEARMONTH									
				,TYPE							
				,CUS_TYPE	
				,SEGMENT
				,PRODUCT_LV_4		
				,PRODUCT_LV_3				
				,PRODUCT_LV_2
				,PRODUCT_LV_1			
				,MEASURE							
				,LEVEL							


----------------- Update 20251125: SME - EOP CUS_SUM ALL PRODUCT  ----------------------------------	
		INSERT INTO ANALYSIS..FORECAST_2026_CALC								
		SELECT  BIZLINE, YEARMONTH									
				,TYPE
				,CUS_TYPE						
				,SEGMENT
				,PRODUCT_LV_4
				,PRODUCT_LV_3 
				,PRODUCT_LV_2
				,PRODUCT_LV_1			
				,MEASURE+'_SUM'							
				,LEVEL							
				,SUM(VALUE) VALUE							
		FROM ANALYSIS..FORECAST_2026_CALC 
		WHERE (MEASURE IN ( 'AVG BAL', 'EOP CUS', 'ACTIVE CUS')  AND BIZLINE = 'SME')    
		GROUP BY BIZLINE, YEARMONTH									
				,TYPE							
				,CUS_TYPE	
				,SEGMENT
				,PRODUCT_LV_4
				,PRODUCT_LV_3 		
				,PRODUCT_LV_2
				,PRODUCT_LV_1			
				,MEASURE							
				,LEVEL	


----------------- 20250925: Update %Register cus, %MTB, %Truly Active, %Low Active CASA&AE ----------------------------------									

		INSERT INTO ANALYSIS..FORECAST_2026_CALC            									
		SELECT   A.BIZLINE
				,A.YEARMONTH									
				,A.TYPE							
				,A.CUS_TYPE						
				,A.SEGMENT
				,A.PRODUCT_LV_4		
				,A.PRODUCT_LV_3				
				,A.PRODUCT_LV_2
				,A.PRODUCT_LV_1			
				,'%' + A.MEASURE 						
				,'L2.5' LEVEL							
				,CASE WHEN B.VALUE=0 THEN 0 
					  WHEN A.VALUE>B.VALUE THEN 1
					  ELSE A.VALUE/B.VALUE END VALUE						
		FROM									
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC 							
					  WHERE MEASURE IN ('Register Cus','Truly Active Cus','Low Active Cus','MTB Cus') 
					        AND PRODUCT_LV_3 = 'CASA&AE' AND BIZLINE = 'Retail' 					
			 )A								
			INNER JOIN								
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC 								
					  WHERE MEASURE IN ('EOP Cus') AND PRODUCT_LV_3 = 'CASA&AE' AND BIZLINE = 'Retail' 			
			 )B								
				ON  A.PRODUCT_LV_4	=B.PRODUCT_LV_4							
				AND A.PRODUCT_LV_3	=B.PRODUCT_LV_3							
				AND A.PRODUCT_LV_2	=B.PRODUCT_LV_2							
				AND A.PRODUCT_LV_1	=B.PRODUCT_LV_1							
				AND A.LEVEL			=B.LEVEL							
				AND A.YEARMONTH		=B.YEARMONTH							
				AND A.TYPE			=B.TYPE							
				AND A.CUS_TYPE		=B.CUS_TYPE							
				AND A.SEGMENT		=B.SEGMENT	
				AND A.BIZLINE		=B.BIZLINE	

----------------- 20250925: Update %Utilization SME ----------------------------------									

		INSERT INTO ANALYSIS..FORECAST_2026_CALC      									
		SELECT   A.BIZLINE
				,A.YEARMONTH									
				,A.TYPE							
				,A.CUS_TYPE						
				,A.SEGMENT
				,A.PRODUCT_LV_4		
				,A.PRODUCT_LV_3				
				,A.PRODUCT_LV_2
				,A.PRODUCT_LV_1			
				,'%Utilization' 						
				,A.LEVEL							
				,CASE WHEN B.VALUE=0 THEN NULL ELSE (A.VALUE/B.VALUE) END VALUE							
		FROM									
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC  	 								
					  WHERE MEASURE IN ('EOP Bal') AND BIZLINE IN ('SME')					
			 )A								
			INNER JOIN								
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC  									
					  WHERE MEASURE IN ('Limit Approved') AND BIZLINE IN ('SME')			
			 )B								
				ON  A.PRODUCT_LV_4	=B.PRODUCT_LV_4							
				AND A.PRODUCT_LV_3	=B.PRODUCT_LV_3							
				AND A.PRODUCT_LV_2	=B.PRODUCT_LV_2							
				AND A.PRODUCT_LV_1	=B.PRODUCT_LV_1							
				AND A.LEVEL			=B.LEVEL							
				AND A.YEARMONTH		=B.YEARMONTH							
				AND A.TYPE			=B.TYPE							
				AND A.CUS_TYPE		=B.CUS_TYPE							
				AND A.SEGMENT		=B.SEGMENT	
				AND A.BIZLINE		=B.BIZLINE	

----------------- 20250925: Update %Utilization Retail ----------------------------------									

		INSERT INTO ANALYSIS..FORECAST_2026_CALC 	      									
		SELECT   A.BIZLINE
				,A.YEARMONTH									
				,A.TYPE							
				,A.CUS_TYPE						
				,A.SEGMENT
				,A.PRODUCT_LV_4		
				,A.PRODUCT_LV_3				
				,A.PRODUCT_LV_2
				,A.PRODUCT_LV_1			
				,'%Utilization' 						
				,A.LEVEL							
				,CASE WHEN B.VALUE=0 THEN NULL ELSE (A.VALUE/B.VALUE) END VALUE							
		FROM									
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC  								
					  WHERE MEASURE IN ('EOP Bal') AND BIZLINE IN ('Retail')					
			 )A								
			INNER JOIN								
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC  									
					  WHERE MEASURE IN ('Limit') AND BIZLINE IN ('Retail')			
			 )B								
				ON  A.PRODUCT_LV_4	=B.PRODUCT_LV_4							
				AND A.PRODUCT_LV_3	=B.PRODUCT_LV_3							
				AND A.PRODUCT_LV_2	=B.PRODUCT_LV_2							
				AND A.PRODUCT_LV_1	=B.PRODUCT_LV_1							
				AND A.LEVEL			=B.LEVEL							
				AND A.YEARMONTH		=B.YEARMONTH							
				AND A.TYPE			=B.TYPE							
				AND A.CUS_TYPE		=B.CUS_TYPE							
				AND A.SEGMENT		=B.SEGMENT	
				AND A.BIZLINE		=B.BIZLINE	


	----------================== C. FULL YEAR + MTD + YTD + % DRIVERS ------------------------------------------------									


		INSERT INTO ANALYSIS..FORECAST_2026_CALC									
		-----------------------------------FY--------------------------------------------------									
		SELECT   BIZLINE, LEFT(YEARMONTH,4)+' FY' YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,PRODUCT_LV_4		
				,PRODUCT_LV_3				
				,PRODUCT_LV_2
				,PRODUCT_LV_1			
				,MEASURE							
				,LEVEL							
				,SUM(	 CASE WHEN MEASURE NOT IN ('AVG BAL','AVG BAL_SUM','EOP BAL','EOP CUS','EOP CUS ACTIVE','EOP CUS HOLDING','IB BAL','AUM','EOP Cus by product','Active cus', 'EOP Cus having limit', 'EOP CUS_SUM'
							                      ,'Register Cus','Truly Active Cus','Low Active Cus','MTB Cus','New Cus CASA&AE','Limit','Limit Approved', '%Register cus', '%MTB Cus', '%Truly Active Cus', '%Low Active Cus','%Utilization') THEN VALUE
							  WHEN (MEASURE IN ('EOP BAL','AUM','EOP Cus','Active cus','Register Cus','Truly Active Cus','Low Active Cus','MTB Cus','New Cus CASA&AE','Limit','Limit Approved'
												  , '%Register cus', '%MTB Cus', '%Truly Active Cus', '%Low Active Cus','%Utilization') OR MEASURE LIKE 'EOP Cus%') AND RIGHT(YEARMONTH,2) = '12' THEN VALUE 
							  WHEN MEASURE IN ('AVG BAL','IB BAL', 'AVG BAL_SUM') THEN VALUE/12
							  ELSE NULL 
						 END )
		FROM ANALYSIS..FORECAST_2026_CALC	
		GROUP BY BIZLINE, LEFT(YEARMONTH,4) + ' FY'									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,PRODUCT_LV_4		
				,PRODUCT_LV_3				
				,PRODUCT_LV_2
				,PRODUCT_LV_1			
				,MEASURE							
				,LEVEL									
		
		-----------------------------------MTD--------------------------------------------------									
									
		UNION ALL									
		SELECT   BIZLINE,YEARMONTH+' MTD' YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,PRODUCT_LV_4		
				,PRODUCT_LV_3				
				,PRODUCT_LV_2
				,PRODUCT_LV_1			
				,MEASURE							
				,LEVEL							
				,SUM(VALUE) VALUE 							
		FROM ANALYSIS..FORECAST_2026_CALC									
		WHERE RIGHT(YEARMONTH,2)=RIGHT(@YTD_MONTH,2)		----- 20260423: YTD							
		GROUP BY BIZLINE,YEARMONTH+' MTD'									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,PRODUCT_LV_4		
				,PRODUCT_LV_3				
				,PRODUCT_LV_2
				,PRODUCT_LV_1			
				,MEASURE							
				,LEVEL							
									
		-----------------------------------YTD--------------------------------------------------									
									
		UNION ALL									
		SELECT   BIZLINE,LEFT(YEARMONTH,4)+RIGHT(@YTD_MONTH,2)+' YTD' YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,PRODUCT_LV_4		
				,PRODUCT_LV_3				
				,PRODUCT_LV_2
				,PRODUCT_LV_1			
				,MEASURE							
				,LEVEL							
				,SUM(	 CASE WHEN MEASURE NOT IN ('AVG BAL','AVG BAL_SUM','EOP BAL','EOP CUS','EOP CUS ACTIVE','EOP CUS HOLDING','IB BAL','AUM','EOP Cus by product','Active cus', 'EOP Cus having limit', 'EOP CUS_SUM'
							                      ,'Register Cus','Truly Active Cus','Low Active Cus','MTB Cus','New Cus CASA&AE','Limit','Limit Approved', '%Register cus', '%MTB Cus', '%Truly Active Cus', '%Low Active Cus','%Utilization') THEN VALUE
							  WHEN (MEASURE IN ('EOP BAL','AUM','EOP Cus','Active cus','Register Cus','Truly Active Cus','Low Active Cus','MTB Cus','New Cus CASA&AE','Limit','Limit Approved'
												  , '%Register cus', '%MTB Cus', '%Truly Active Cus', '%Low Active Cus','%Utilization') OR MEASURE LIKE 'EOP Cus%') AND RIGHT(YEARMONTH,2) = '12'  AND CAST(LEFT(YEARMONTH,4) AS INT) < 2026 THEN VALUE
							  WHEN (MEASURE IN ('EOP BAL','AUM','EOP Cus','Active cus','Register Cus','Truly Active Cus','Low Active Cus','MTB Cus','New Cus CASA&AE','Limit','Limit Approved'
												  , '%Register cus', '%MTB Cus', '%Truly Active Cus', '%Low Active Cus','%Utilization') OR MEASURE LIKE 'EOP Cus%') AND RIGHT(YEARMONTH,2)= RIGHT(@YTD_MONTH,2) AND CAST(LEFT(YEARMONTH,4) AS INT) = 2026 THEN VALUE
							  WHEN MEASURE IN ('AVG BAL','IB BAL') THEN VALUE/CAST(RIGHT(@YTD_MONTH,2) AS INT)
							  ELSE NULL 
						 END )
		FROM ANALYSIS..FORECAST_2026_CALC									
		WHERE YEARMONTH NOT IN ('2024 FY','2025 FY','2026 FY') AND CAST(YEARMONTH AS INT) <= CAST(@YTD_MONTH AS INT)		----- 20260423: YTD							
		GROUP BY BIZLINE,LEFT(YEARMONTH,4)+RIGHT(@YTD_MONTH,2)+' YTD'								
				,TYPE							
				,CUS_TYPE,SEGMENT
				,PRODUCT_LV_4		
				,PRODUCT_LV_3				
				,PRODUCT_LV_2
				,PRODUCT_LV_1			
				,MEASURE							
				,LEVEL										
									
----------------------------------CALCULATION 1.2 %NIM,%CI,%CI_ADJ,%INT_REV,%FTP----------------------------------									

		INSERT INTO ANALYSIS..FORECAST_2026_CALC       									
		SELECT   A.BIZLINE
				,A.YEARMONTH									
				,A.TYPE							
				,A.CUS_TYPE						
				,A.SEGMENT
				,A.PRODUCT_LV_4		
				,A.PRODUCT_LV_3				
				,A.PRODUCT_LV_2
				,A.PRODUCT_LV_1			
				,'%'+A.MEASURE 							
				,A.LEVEL							
				,CASE WHEN B.VALUE=0 THEN 0 ELSE (A.VALUE/B.VALUE)*(365/C.NO_DAY) END VALUE							
		FROM									
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC								
					  WHERE MEASURE IN ('CI','CI_ADJ','INT_REV','FTP','CI_Normal','Liquidity Cost')						
							AND PRODUCT_LV_4 IN ('TOTAL LENDING/CARD','TOTAL DEPOSIT')					
			 )A								
			INNER JOIN								
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC								
					  WHERE MEASURE ='AVG BAL'						
							AND PRODUCT_LV_4 IN ('TOTAL LENDING/CARD','TOTAL DEPOSIT')					
			 )B								
				ON  A.PRODUCT_LV_4	=B.PRODUCT_LV_4							
				AND A.PRODUCT_LV_3	=B.PRODUCT_LV_3							
				AND A.PRODUCT_LV_2	=B.PRODUCT_LV_2							
				AND A.PRODUCT_LV_1	=B.PRODUCT_LV_1							
				AND A.LEVEL			=B.LEVEL							
				AND A.YEARMONTH		=B.YEARMONTH							
				AND A.TYPE			=B.TYPE							
				AND A.CUS_TYPE		=B.CUS_TYPE							
				AND A.SEGMENT		=B.SEGMENT	
				AND A.BIZLINE		=B.BIZLINE	
									
			LEFT JOIN ANALYSIS..MAPPING_DAYS C ON A.YEARMONTH=C.YEARMONTH								
									
			------ %NIM (Budget line adjustment) -- 20260210
			INSERT INTO ANALYSIS..FORECAST_2026_CALC
			SELECT   A.BIZLINE
				,A.YEARMONTH									
				,A.TYPE							
				,A.CUS_TYPE						
				,A.SEGMENT
				,A.PRODUCT_LV_4		
				,A.PRODUCT_LV_3				
				,A.PRODUCT_LV_2
				,A.PRODUCT_LV_1			
				,'%NIM'							
				,A.LEVEL							
				,CASE WHEN B.VALUE=0 THEN 0 ELSE (A.VALUE/B.VALUE)*(365/C.NO_DAY) END VALUE							
		FROM									
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC 								
					  WHERE (MEASURE IN ('NII') AND PRODUCT_LV_4 IN ('TOTAL DEPOSIT'))			  --- Note: FI k đưa vào %NIM, CI _adj đưa vào OPEX			
							OR (MEASURE IN ('CI','INT_REV','FTP') AND PRODUCT_LV_4 IN ('TOTAL LENDING/CARD'))					
			 )A								
			INNER JOIN								
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC 								
					  WHERE MEASURE ='AVG BAL'						
							AND PRODUCT_LV_4 IN ('TOTAL LENDING/CARD','TOTAL DEPOSIT')					
			 )B								
				ON  A.PRODUCT_LV_4	=B.PRODUCT_LV_4							
				AND A.PRODUCT_LV_3	=B.PRODUCT_LV_3							
				AND A.PRODUCT_LV_2	=B.PRODUCT_LV_2							
				AND A.PRODUCT_LV_1	=B.PRODUCT_LV_1							
				AND A.LEVEL			=B.LEVEL							
				AND A.YEARMONTH		=B.YEARMONTH							
				AND A.TYPE			=B.TYPE							
				AND A.CUS_TYPE		=B.CUS_TYPE							
				AND A.SEGMENT		=B.SEGMENT	
				AND A.BIZLINE		=B.BIZLINE
									
			LEFT JOIN ANALYSIS..MAPPING_DAYS C ON A.YEARMONTH=C.YEARMONTH

		---------SME: %Margin  - Update 20250925 ----------------------------
	
		INSERT INTO ANALYSIS..FORECAST_2026_CALC       									
			
		SELECT   A.BIZLINE
					,A.YEARMONTH									
					,A.TYPE							
					,A.CUS_TYPE						
					,A.SEGMENT
					,A.PRODUCT_LV_4		
					,A.PRODUCT_LV_3				
					,A.PRODUCT_LV_2
					,A.PRODUCT_LV_1			
					,'%Margin'							
					,A.LEVEL							
					,CASE WHEN B.VALUE=0 THEN 0 ELSE (A.VALUE/B.VALUE)*(365/C.NO_DAY) END VALUE							
			FROM									
				(SELECT * FROM ANALYSIS..FORECAST_2026_CALC 								
						  WHERE (MEASURE = 'NII' AND PRODUCT_LV_1 IN ('LC Interest ','LG Interest '))			  			
								AND BIZLINE = 'SME'					
				 )A								
				INNER JOIN								
				(SELECT * FROM ANALYSIS..FORECAST_2026_CALC								
						  WHERE MEASURE ='AVG BAL' AND  PRODUCT_LV_1 IN ('LC Interest ','LG Interest ')					
								AND BIZLINE = 'SME'
				 )B								
					ON  A.PRODUCT_LV_4	= B.PRODUCT_LV_4							
					AND A.PRODUCT_LV_3	= B.PRODUCT_LV_3							
					AND A.PRODUCT_LV_2	= B.PRODUCT_LV_2							
					AND A.PRODUCT_LV_1	= B.PRODUCT_LV_1							
					AND A.LEVEL			= B.LEVEL							
					AND A.YEARMONTH		= B.YEARMONTH							
					AND A.TYPE			= B.TYPE							
					AND A.CUS_TYPE		= B.CUS_TYPE							
					AND A.SEGMENT		= B.SEGMENT	
					AND A.BIZLINE		= B.BIZLINE
									
			LEFT JOIN ANALYSIS..MAPPING_DAYS C ON A.YEARMONTH = C.YEARMONTH	


			UNION ALL 
	
			SELECT   A.BIZLINE
						,A.YEARMONTH									
						,A.TYPE							
						,A.CUS_TYPE						
						,A.SEGMENT
						,A.PRODUCT_LV_4		
						,A.PRODUCT_LV_3				
						,A.PRODUCT_LV_2
						,A.PRODUCT_LV_1			
						,'%Margin'							
						,A.LEVEL							
						,CASE WHEN B.VALUE=0 THEN 0 ELSE (A.VALUE/B.VALUE)*(365/C.NO_DAY) END VALUE							
				FROM									
					(SELECT * FROM ANALYSIS..FORECAST_2026_CALC 								
							  WHERE (MEASURE = 'NFI' AND PRODUCT_LV_1 IN ('LG Fee','LC Fee'))			  			
									AND BIZLINE = 'SME'					
					 )A								
					INNER JOIN								
					(SELECT * FROM ANALYSIS..FORECAST_2026_CALC								
							  WHERE MEASURE ='AVG BAL' AND PRODUCT_LV_1 IN ('LG Fee','LC Fee')			
									AND BIZLINE = 'SME'
					 )B								
						ON  A.PRODUCT_LV_4	= B.PRODUCT_LV_4							
						AND A.PRODUCT_LV_3	= B.PRODUCT_LV_3							
						AND A.PRODUCT_LV_2	= B.PRODUCT_LV_2							
						AND A.PRODUCT_LV_1	= B.PRODUCT_LV_1							
						AND A.LEVEL			= B.LEVEL							
						AND A.YEARMONTH		= B.YEARMONTH							
						AND A.TYPE			= B.TYPE							
						AND A.CUS_TYPE		= B.CUS_TYPE							
						AND A.SEGMENT		= B.SEGMENT	
						AND A.BIZLINE		= B.BIZLINE
									
				LEFT JOIN ANALYSIS..MAPPING_DAYS C ON A.YEARMONTH = C.YEARMONTH	

----------------------------------CALCULATION % ACTIVE CASA + CARD----------------------------------									


		INSERT INTO ANALYSIS..FORECAST_2026_CALC        									
		SELECT   A.BIZLINE
				,A.YEARMONTH									
				,A.TYPE							
				,A.CUS_TYPE						
				,A.SEGMENT
				,A.PRODUCT_LV_4		
				,A.PRODUCT_LV_3				
				,A.PRODUCT_LV_2
				,A.PRODUCT_LV_1			
				,'%Active CASA' 						
				,A.LEVEL							
				,CASE WHEN B.VALUE=0 THEN 0 
					  WHEN A.VALUE>B.VALUE THEN 1
					  ELSE A.VALUE/B.VALUE END VALUE							
		FROM									
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC  								
					  WHERE MEASURE IN ('ACTIVE CUS_SUM') AND BIZLINE = 'SME' AND PRODUCT_LV_3 IN ('CASA')	--- 20250925: Update			
			 )A								
			INNER JOIN								
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC  								
					  WHERE MEASURE IN ('EOP CUS_SUM') AND PRODUCT_LV_3 IN ('CASA')	AND BIZLINE = 'SME'	
			 )B								
				ON  A.PRODUCT_LV_4	=B.PRODUCT_LV_4							
				AND A.PRODUCT_LV_3	=B.PRODUCT_LV_3							
				AND A.PRODUCT_LV_2	=B.PRODUCT_LV_2							
				AND A.PRODUCT_LV_1	=B.PRODUCT_LV_1							
				AND A.LEVEL			=B.LEVEL							
				AND A.YEARMONTH		=B.YEARMONTH							
				AND A.TYPE			=B.TYPE							
				AND A.CUS_TYPE		=B.CUS_TYPE							
				AND A.SEGMENT		=B.SEGMENT	
				AND A.BIZLINE		=B.BIZLINE
									

		INSERT INTO ANALYSIS..FORECAST_2026_CALC        									
		SELECT   A.BIZLINE
				,A.YEARMONTH									
				,A.TYPE							
				,A.CUS_TYPE						
				,A.SEGMENT
				,A.PRODUCT_LV_4		
				,A.PRODUCT_LV_3				
				,A.PRODUCT_LV_2
				,A.PRODUCT_LV_1			
				,'%Active Card' 						
				,A.LEVEL							
				,CASE WHEN B.VALUE=0 THEN 0 ELSE (A.VALUE/B.VALUE) END VALUE							
		FROM									
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC 								
					  WHERE MEASURE IN ('ACTIVE CARD') AND PRODUCT_LV_3 IN ('CARD')					
			 )A								
			INNER JOIN								
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC  								
					  WHERE MEASURE IN ('EOP CARD') AND PRODUCT_LV_3 IN ('CARD')				
			 )B								
				ON  A.PRODUCT_LV_4	=B.PRODUCT_LV_4							
				AND A.PRODUCT_LV_3	=B.PRODUCT_LV_3							
				AND A.PRODUCT_LV_2	=B.PRODUCT_LV_2							
				AND A.PRODUCT_LV_1	=B.PRODUCT_LV_1							
				AND A.LEVEL			=B.LEVEL							
				AND A.YEARMONTH		=B.YEARMONTH							
				AND A.TYPE			=B.TYPE							
				AND A.CUS_TYPE		=B.CUS_TYPE							
				AND A.SEGMENT		=B.SEGMENT	
				AND A.BIZLINE		=B.BIZLINE
																		
----------------- 20260410: Update Ticket size SME ----------------------------------									

		INSERT INTO ANALYSIS..FORECAST_2026_CALC        									
		SELECT   A.BIZLINE
				,A.YEARMONTH									
				,A.TYPE							
				,A.CUS_TYPE						
				,A.SEGMENT
				,A.PRODUCT_LV_4		
				,A.PRODUCT_LV_3				
				,A.PRODUCT_LV_2
				,A.PRODUCT_LV_1			
				,'Ticket size' 						
				,A.LEVEL							
				,CASE WHEN B.VALUE=0 THEN NULL ELSE (A.VALUE/B.VALUE) END VALUE							
		FROM									
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC 								
					  WHERE MEASURE IN ('AVG BAL_SUM') AND BIZLINE IN ('SME')					
			 )A								
			INNER JOIN								
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC 								
					  WHERE MEASURE IN ('EOP CUS_SUM') AND BIZLINE IN ('SME')			
			 )B								
				ON  A.PRODUCT_LV_4	=B.PRODUCT_LV_4							
				AND A.PRODUCT_LV_3	=B.PRODUCT_LV_3							
				AND A.PRODUCT_LV_2	=B.PRODUCT_LV_2							
				AND A.PRODUCT_LV_1	=B.PRODUCT_LV_1							
				AND A.LEVEL			=B.LEVEL							
				AND A.YEARMONTH		=B.YEARMONTH							
				AND A.TYPE			=B.TYPE							
				AND A.CUS_TYPE		=B.CUS_TYPE							
				AND A.SEGMENT		=B.SEGMENT	
				AND A.BIZLINE		=B.BIZLINE	

----------------- 20251125: Update %NFI/Txn Amt SME ----------------------------------									

		INSERT INTO ANALYSIS..FORECAST_2026_CALC         									
		SELECT   A.BIZLINE
				,A.YEARMONTH									
				,A.TYPE							
				,A.CUS_TYPE						
				,A.SEGMENT
				,A.PRODUCT_LV_4		
				,A.PRODUCT_LV_3				
				,A.PRODUCT_LV_2
				,A.PRODUCT_LV_1			
				,'%NFI/Txn Amt' 						
				,A.LEVEL							
				,CASE WHEN B.VALUE=0 THEN NULL ELSE (A.VALUE/B.VALUE) END VALUE							
		FROM									
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC								
					  WHERE MEASURE IN ('NFI') AND BIZLINE IN ('SME')					
			 )A								
			INNER JOIN								
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC 								
					  WHERE MEASURE IN ('Tnx Amt', 'Txn Amt') AND BIZLINE IN ('SME')			
			 )B								
				ON  A.PRODUCT_LV_4	=B.PRODUCT_LV_4							
				AND A.PRODUCT_LV_3	=B.PRODUCT_LV_3							
				AND A.PRODUCT_LV_2	=B.PRODUCT_LV_2							
				AND A.PRODUCT_LV_1	=B.PRODUCT_LV_1							
				AND A.LEVEL			=B.LEVEL							
				AND A.YEARMONTH		=B.YEARMONTH							
				AND A.TYPE			=B.TYPE							
				AND A.CUS_TYPE		=B.CUS_TYPE							
				AND A.SEGMENT		=B.SEGMENT	
				AND A.BIZLINE		=B.BIZLINE	
				
----------------- 20250925: Update %Trading Income/Txn Amt SME ----------------------------------									

		INSERT INTO ANALYSIS..FORECAST_2026_CALC        									
		SELECT   A.BIZLINE
				,A.YEARMONTH									
				,A.TYPE							
				,A.CUS_TYPE						
				,A.SEGMENT
				,A.PRODUCT_LV_4		
				,A.PRODUCT_LV_3				
				,A.PRODUCT_LV_2
				,A.PRODUCT_LV_1			
				,'%Trading Income/Txn Amt' 						
				,A.LEVEL							
				,CASE WHEN B.VALUE=0 THEN NULL ELSE (A.VALUE/B.VALUE) END VALUE							
		FROM									
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC  								
					  WHERE MEASURE IN ('Trading Income') AND BIZLINE IN ('SME')					
			 )A								
			INNER JOIN								
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC 							
					  WHERE MEASURE IN ('Tnx Amt', 'Txn Amt') AND BIZLINE IN ('SME')			
			 )B								
				ON  A.PRODUCT_LV_4	=B.PRODUCT_LV_4							
				AND A.PRODUCT_LV_3	=B.PRODUCT_LV_3							
				AND A.PRODUCT_LV_2	=B.PRODUCT_LV_2							
				AND A.PRODUCT_LV_1	=B.PRODUCT_LV_1							
				AND A.LEVEL			=B.LEVEL							
				AND A.YEARMONTH		=B.YEARMONTH							
				AND A.TYPE			=B.TYPE							
				AND A.CUS_TYPE		=B.CUS_TYPE							
				AND A.SEGMENT		=B.SEGMENT	
				AND A.BIZLINE		=B.BIZLINE	

----------------- 20250925: Update %NFI/New Bal for investment product ----------------------------------									

		INSERT INTO ANALYSIS..FORECAST_2026_CALC             									
		SELECT   A.BIZLINE
				,A.YEARMONTH									
				,A.TYPE							
				,A.CUS_TYPE						
				,A.SEGMENT
				,A.PRODUCT_LV_4		
				,A.PRODUCT_LV_3				
				,A.PRODUCT_LV_2
				,A.PRODUCT_LV_1			
				,'%NFI/New Bal' 						
				,A.LEVEL							
				,CASE WHEN B.VALUE=0 THEN 0 
					  WHEN A.VALUE>B.VALUE THEN 1
					  ELSE A.VALUE/B.VALUE END VALUE						
		FROM									
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC  							
					  WHERE MEASURE IN ('NFI') AND BIZLINE IN ('Retail')					
			 )A								
			INNER JOIN								
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC  								
					  WHERE MEASURE IN ('New Bal') AND BIZLINE IN ('Retail')			
			 )B								
				ON  A.PRODUCT_LV_4	=B.PRODUCT_LV_4							
				AND A.PRODUCT_LV_3	=B.PRODUCT_LV_3							
				AND A.PRODUCT_LV_2	=B.PRODUCT_LV_2							
				AND A.PRODUCT_LV_1	=B.PRODUCT_LV_1							
				AND A.LEVEL			=B.LEVEL							
				AND A.YEARMONTH		=B.YEARMONTH							
				AND A.TYPE			=B.TYPE							
				AND A.CUS_TYPE		=B.CUS_TYPE							
				AND A.SEGMENT		=B.SEGMENT	
				AND A.BIZLINE		=B.BIZLINE	

----------------- 20250925: Update %Fee DPM and Gold ----------------------------------									

		INSERT INTO ANALYSIS..FORECAST_2026_CALC            									
		SELECT   A.BIZLINE
				,A.YEARMONTH									
				,A.TYPE							
				,A.CUS_TYPE						
				,A.SEGMENT
				,A.PRODUCT_LV_4		
				,A.PRODUCT_LV_3				
				,A.PRODUCT_LV_2
				,A.PRODUCT_LV_1			
				,'%Fee' 						
				,A.LEVEL							
				,CASE WHEN B.VALUE=0 THEN 0 
					  WHEN A.VALUE>B.VALUE THEN 1
					  ELSE A.VALUE/B.VALUE END VALUE							
		FROM									
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC   							
					  WHERE MEASURE IN ('NFI') AND BIZLINE IN ('Retail')					
			 )A								
			INNER JOIN								
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC   								
					  WHERE MEASURE IN ('EOP BAL') AND BIZLINE IN ('Retail')			
			 )B								
				ON  A.PRODUCT_LV_4	=B.PRODUCT_LV_4							
				AND A.PRODUCT_LV_3	=B.PRODUCT_LV_3							
				AND A.PRODUCT_LV_2	=B.PRODUCT_LV_2							
				AND A.PRODUCT_LV_1	=B.PRODUCT_LV_1							
				AND A.LEVEL			=B.LEVEL							
				AND A.YEARMONTH		=B.YEARMONTH							
				AND A.TYPE			=B.TYPE							
				AND A.CUS_TYPE		=B.CUS_TYPE							
				AND A.SEGMENT		=B.SEGMENT	
				AND A.BIZLINE		=B.BIZLINE		

----------------- 20250925: Update %NFI/APE for Banca ----------------------------------									

		INSERT INTO ANALYSIS..FORECAST_2026_CALC             									
		SELECT   A.BIZLINE
				,A.YEARMONTH									
				,A.TYPE							
				,A.CUS_TYPE						
				,A.SEGMENT
				,A.PRODUCT_LV_4		
				,A.PRODUCT_LV_3				
				,A.PRODUCT_LV_2
				,A.PRODUCT_LV_1			
				,'%NFI/APE' 						
				,A.LEVEL							
				,CASE WHEN B.VALUE=0 THEN 0 
					  WHEN A.VALUE>B.VALUE THEN 1
					  ELSE A.VALUE/B.VALUE END VALUE						
		FROM									
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC   							
					  WHERE MEASURE IN ('NFI') 					
			 )A								
			INNER JOIN								
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC   								
					  WHERE MEASURE IN ('APE') 			
			 )B								
				ON  A.PRODUCT_LV_4	=B.PRODUCT_LV_4							
				AND A.PRODUCT_LV_3	=B.PRODUCT_LV_3							
				AND A.PRODUCT_LV_2	=B.PRODUCT_LV_2							
				AND A.PRODUCT_LV_1	=B.PRODUCT_LV_1							
				AND A.LEVEL			=B.LEVEL							
				AND A.YEARMONTH		=B.YEARMONTH							
				AND A.TYPE			=B.TYPE							
				AND A.CUS_TYPE		=B.CUS_TYPE							
				AND A.SEGMENT		=B.SEGMENT	
				AND A.BIZLINE		=B.BIZLINE	

----------------- 20250925: Update %NFI/GWP for Banca ----------------------------------									

		INSERT INTO ANALYSIS..FORECAST_2026_CALC            									
		SELECT   A.BIZLINE
				,A.YEARMONTH									
				,A.TYPE							
				,A.CUS_TYPE						
				,A.SEGMENT
				,A.PRODUCT_LV_4		
				,A.PRODUCT_LV_3				
				,A.PRODUCT_LV_2
				,A.PRODUCT_LV_1			
				,'%NFI/GWP' 						
				,A.LEVEL							
				,CASE WHEN B.VALUE=0 THEN 0 
					  WHEN A.VALUE>B.VALUE THEN 1
					  ELSE A.VALUE/B.VALUE END VALUE						
		FROM									
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC   							
					  WHERE MEASURE IN ('NFI') 					
			 )A								
			INNER JOIN								
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC   								
					  WHERE MEASURE IN ('GWP') 			
			 )B								
				ON  A.PRODUCT_LV_4	=B.PRODUCT_LV_4							
				AND A.PRODUCT_LV_3	=B.PRODUCT_LV_3							
				AND A.PRODUCT_LV_2	=B.PRODUCT_LV_2							
				AND A.PRODUCT_LV_1	=B.PRODUCT_LV_1							
				AND A.LEVEL			=B.LEVEL							
				AND A.YEARMONTH		=B.YEARMONTH							
				AND A.TYPE			=B.TYPE							
				AND A.CUS_TYPE		=B.CUS_TYPE							
				AND A.SEGMENT		=B.SEGMENT	
				AND A.BIZLINE		=B.BIZLINE	



----------------- 20250925: Update %NFI/PV ----------------------------------									

		INSERT INTO ANALYSIS..FORECAST_2026_CALC             									
		SELECT   A.BIZLINE
				,A.YEARMONTH									
				,A.TYPE							
				,A.CUS_TYPE						
				,A.SEGMENT
				,A.PRODUCT_LV_4		
				,A.PRODUCT_LV_3				
				,A.PRODUCT_LV_2
				,A.PRODUCT_LV_1			
				,'NFI/PV' 						
				,A.LEVEL							
				,CASE WHEN B.VALUE=0 THEN 0 
					  WHEN A.VALUE>B.VALUE THEN 1
					  ELSE A.VALUE/B.VALUE END VALUE						
		FROM									
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC   WHERE MEASURE IN ('NFI') 	AND PRODUCT_LV_3 = 'Card' AND BIZLINE = 'Retail' 				
			 )A								
			INNER JOIN								
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC   								
					  WHERE MEASURE IN ('Purchase Amt') AND PRODUCT_LV_3 = 'Card' AND BIZLINE = 'Retail' 			
			 )B								
				ON  A.PRODUCT_LV_4	=B.PRODUCT_LV_4							
				AND A.PRODUCT_LV_3	=B.PRODUCT_LV_3							
				AND A.PRODUCT_LV_2	=B.PRODUCT_LV_2							
				AND A.PRODUCT_LV_1	=B.PRODUCT_LV_1							
				AND A.LEVEL			=B.LEVEL							
				AND A.YEARMONTH		=B.YEARMONTH							
				AND A.TYPE			=B.TYPE							
				AND A.CUS_TYPE		=B.CUS_TYPE							
				AND A.SEGMENT		=B.SEGMENT	
				AND A.BIZLINE		=B.BIZLINE	


----------------------------------CALCULATION 1.3 %PENETRATION ----------------------------------									

		INSERT INTO ANALYSIS..FORECAST_2026_CALC       									
		SELECT   A.BIZLINE
				,A.YEARMONTH									
				,A.TYPE							
				,A.CUS_TYPE						
				,A.SEGMENT
				,A.PRODUCT_LV_4		
				,A.PRODUCT_LV_3				
				,A.PRODUCT_LV_2
				,A.PRODUCT_LV_1			
				,'%PENETRATION'							
				,A.LEVEL							
				,CASE WHEN B.VALUE=0 THEN 0 
					  WHEN A.VALUE>B.VALUE THEN 1
					  ELSE A.VALUE/B.VALUE END VALUE							
		FROM									
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC								
					  WHERE MEASURE IN ('EOP CUS')					
							AND PRODUCT_LV_4 <> 'CUS'
			 )A								
			INNER JOIN								
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC								
					  WHERE MEASURE IN ('EOP CUS HOLDING')					
							AND PRODUCT_LV_4 = 'CUS'				
							AND LEVEL = 'L1' ----------- 20260410: Update
			 )B								
				ON  A.YEARMONTH		=B.YEARMONTH							
				AND A.TYPE			=B.TYPE							
				AND A.CUS_TYPE		=B.CUS_TYPE							
				AND A.SEGMENT		=B.SEGMENT	
				AND A.BIZLINE		=B.BIZLINE	
	


----------------------------------CALCULATION 1.3.2 %PENETRATION  OF CD (20230929)----------------------------------									

		INSERT INTO ANALYSIS..FORECAST_2026_CALC        									
		SELECT   A.BIZLINE
				,A.YEARMONTH									
				,A.TYPE							
				,A.CUS_TYPE						
				,A.SEGMENT
				,A.PRODUCT_LV_4		
				,A.PRODUCT_LV_3				
				,A.PRODUCT_LV_2
				,A.PRODUCT_LV_1			
				,'%PENETRATION'							
				,A.LEVEL							
				,CASE WHEN B.VALUE=0 THEN 0 
					  WHEN A.VALUE>B.VALUE THEN 1
					  ELSE A.VALUE/B.VALUE END VALUE							
		FROM									
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC 								
					  WHERE MEASURE IN ('EOP Cus by product')					
							AND PRODUCT_LV_2 = 'CD'
			 )A								
			INNER JOIN								
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC 								
					  WHERE MEASURE IN ('EOP CUS HOLDING')					
							AND PRODUCT_LV_4 = 'CUS'				
							AND LEVEL = 'L1' ----------- 20260410: Update
			 )B								
				ON  A.YEARMONTH		=B.YEARMONTH							
				AND A.TYPE			=B.TYPE							
				AND A.CUS_TYPE		=B.CUS_TYPE							
				AND A.SEGMENT		=B.SEGMENT	
				AND A.BIZLINE		=B.BIZLINE	


---------  20250925: Update Other asset EOP Bal -----------------
		 INSERT INTO ANALYSIS..FORECAST_2026_CALC       			
		 SELECT BIZLINE, MONTH, TYPE
			  ,' ALL' CUS_TYPE, ' ALL' SEGMENT
			  ,'Total Deposit' , PRODUCT_FAMILY, PRODUCT_LINE, SUB_PRODUCT  
			  , MEASURE
			  ,'L1.5' 
			  ,VALUE
		 FROM ##FORECAST_CONSOL
		 WHERE PRODUCT_LINE = 'Other Asset' AND MEASURE = 'EOP BAL'

		 INSERT INTO ANALYSIS..FORECAST_2026_CALC        			
		 SELECT BIZLINE, MONTH, TYPE
			  ,' ALL' CUS_TYPE, ' ALL' SEGMENT
			  ,'Total Deposit' , PRODUCT_FAMILY, PRODUCT_LINE, 'All'  
			  , MEASURE
			  ,'L2.5' 
			  ,sum(VALUE)
		 FROM ##FORECAST_CONSOL
		 WHERE PRODUCT_LINE = 'Other Asset' AND MEASURE = 'EOP BAL'
		 GROUP BY BIZLINE, MONTH, TYPE, PRODUCT_FAMILY, PRODUCT_LINE, MEASURE

		 INSERT INTO ANALYSIS..FORECAST_2026_CALC     
	     SELECT BIZLINE, MONTH, TYPE
				,' ALL' CUS_TYPE, ' ALL' SEGMENT
				,'Total Deposit' , PRODUCT_FAMILY, 'All', 'All'  
				, case when measure = 'EOP Bal' then 'EOP Bal w Other asset'
						when measure = 'Avg Bal' then 'Avg Bal w Other asset' 
				else MEASURE end as measure
				,'L2.5' 
				,sum(VALUE)
		 FROM ##FORECAST_CONSOL
		 WHERE product_family = 'CASA&AE' 
		 and PRODUCT_LINE in ('CASA','CD AE','Other Asset') AND MEASURE in ('EOP BAL','Avg Bal')
		 GROUP BY BIZLINE, MONTH, TYPE, PRODUCT_FAMILY, PRODUCT_LINE, MEASURE

		 INSERT INTO ANALYSIS..FORECAST_2026_CALC    
		
		SELECT   BIZLINE,LEFT(YEARMONTH,4)+RIGHT(@YTD_MONTH,2)+' YTD' YEARMONTH									
				,TYPE							
				,CUS_TYPE,SEGMENT
				,PRODUCT_LV_4		
				,PRODUCT_LV_3				
				,PRODUCT_LV_2
				,PRODUCT_LV_1			
				,MEASURE							
				,LEVEL							
				,SUM( CASE WHEN MEASURE IN ('EOP Bal w Other asset') AND RIGHT(YEARMONTH,2) = '12'  AND CAST(LEFT(YEARMONTH,4) AS INT) < 2026 THEN VALUE
						   WHEN MEASURE IN ('EOP Bal w Other asset') AND RIGHT(YEARMONTH,2)= RIGHT(@YTD_MONTH,2) AND CAST(LEFT(YEARMONTH,4) AS INT) = 2026 THEN VALUE
						   WHEN MEASURE IN ('Avg Bal w Other asset') THEN VALUE/CAST(RIGHT(@YTD_MONTH,2) AS INT)
						   ELSE NULL END ) VALUE
		FROM ANALYSIS..FORECAST_2026_CALC									
		WHERE YEARMONTH NOT IN ('2024 FY','2025 FY','2026 FY') AND CAST(LEFT(YEARMONTH,6) AS INT) <= CAST(@YTD_MONTH AS INT)		----- 20260226: YTD							
		GROUP BY BIZLINE,LEFT(YEARMONTH,4)+RIGHT(@YTD_MONTH,2)+' YTD'								
				,TYPE							
				,CUS_TYPE,SEGMENT
				,PRODUCT_LV_4		
				,PRODUCT_LV_3				
				,PRODUCT_LV_2
				,PRODUCT_LV_1			
				,MEASURE							
				,LEVEL					
				
----------------------------------CALCULATION 1.4: NFX:CALCULATE TRANSACTION PER CUS ----------------------------------	Update: 20241104
		INSERT INTO ANALYSIS..FORECAST_2026_CALC         									
		SELECT   A.BIZLINE
				,A.YEARMONTH									
				,A.TYPE							
				,A.CUS_TYPE						
				,A.SEGMENT
				,A.PRODUCT_LV_4		
				,A.PRODUCT_LV_3				
				,A.PRODUCT_LV_2
				,A.PRODUCT_LV_1			
				,'TXN Amt per cus' 						
				,A.LEVEL							
				,CASE WHEN B.VALUE=0 THEN NULL ELSE (A.VALUE/B.VALUE)*1E3 END VALUE  --UNIT: Mil USD							
		FROM									
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC 								
					  WHERE MEASURE IN ('TXN Amt', 'TNX Amt') AND PRODUCT_LV_1  IN ('FX Flow')					
			 )A								
			INNER JOIN								
			(SELECT * FROM ANALYSIS..FORECAST_2026_CALC 						
					  WHERE MEASURE IN ('EOP Cus') AND PRODUCT_LV_1  IN ('FX Flow')			
			 )B								
				ON  A.PRODUCT_LV_4	=B.PRODUCT_LV_4							
				AND A.PRODUCT_LV_3	=B.PRODUCT_LV_3							
				AND A.PRODUCT_LV_2	=B.PRODUCT_LV_2							
				AND A.PRODUCT_LV_1	=B.PRODUCT_LV_1							
				AND A.LEVEL			=B.LEVEL							
				AND A.YEARMONTH		=B.YEARMONTH							
				AND A.TYPE			=B.TYPE							
				AND A.CUS_TYPE		=B.CUS_TYPE							
				AND A.SEGMENT		=B.SEGMENT	
				AND A.BIZLINE		=B.BIZLINE	

----------------------------------CALCULATION 1.4	2026 - 2025----------------------------------									
/*
		INSERT INTO ANALYSIS..FORECAST_2026_CALC       									
		SELECT   '2026 - 2025'
				,TYPE							
			    ,CUS_TYPE,SEGMENT,PRODUCT_LV_4,PRODUCT_LV_3,PRODUCT_LV_2,PRODUCT_LV_1,MEASURE,LEVEL							
				,CASE WHEN MEASURE IN ('EOP CUS', 'EOP CUS ACTIVE','EOP CUS HOLDING') AND [2023 FY] IS NULL THEN NULL							
					  ELSE ISNULL([2026 FY],0)-ISNULL([2023 FY],0) END						
		FROM (SELECT YEARMONTH
					,'BUDGET'	TYPE					
					,CUS_TYPE,SEGMENT,PRODUCT_LV_4,PRODUCT_LV_3,PRODUCT_LV_2,PRODUCT_LV_1,MEASURE,LEVEL,VALUE			
			  FROM ANALYSIS..FORECAST_2026_CALC
			  WHERE YEARMONTH IN ('2026 FY','2024 FY')
					AND TYPE <> 'TOP_DOWN'
					AND YEARMONTH+TYPE <> '2024 FYBUDGET'
			 ) A
		PIVOT (SUM(VALUE) FOR YEARMONTH IN ([2026 FY],[2024 FY])) PVT
*/						
									
----------------------------------CALCULATION 1.5	2026 / 2025----------------------------------									
/*
		INSERT INTO ANALYSIS..FORECAST_2026_CALC       									
		SELECT   '2026 / 2024'
				,TYPE							
			    ,CUS_TYPE,SEGMENT,PRODUCT_LV_4,PRODUCT_LV_3,PRODUCT_LV_2,PRODUCT_LV_1,MEASURE,LEVEL							
				,CASE WHEN [2024 FY]  <> 0 THEN (ISNULL([2026 FY],0) - ISNULL([2024 FY],0))/ABS(ISNULL([2024 FY],0)) END	
				--,CASE WHEN [2024 FY]  <> 0 THEN ISNULL([2026 FY],0)/ISNULL([2024 FY],0) END											---EDIT

		FROM (SELECT YEARMONTH
					,'BUDGET'	TYPE					
					,CUS_TYPE,SEGMENT,PRODUCT_LV_4,PRODUCT_LV_3,PRODUCT_LV_2,PRODUCT_LV_1,MEASURE,LEVEL,VALUE			
			  FROM ANALYSIS..FORECAST_2026_CALC
			  WHERE YEARMONTH IN ('2026 FY','2024 FY')
					AND TYPE <> 'TOP_DOWN'
					AND YEARMONTH+TYPE <> '2024 FYBUDGET'
			 ) A
		PIVOT (SUM(VALUE) FOR YEARMONTH IN ([2026 FY],[2024 FY])) PVT
		---
*/							
	
----------------------------------Calculation 1.6 Forecast 2026 - Budget 2026----------------------------------					


		INSERT INTO ANALYSIS..FORECAST_2026_CALC       									
		SELECT   BIZLINE,YEARMONTH
				,'FORECAST - BUDGET'							
			    ,CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL							
				,CASE WHEN [FORECAST]  <> 0 THEN ISNULL([FORECAST],0)-ISNULL([BUDGET],0) END			
				
		FROM (	SELECT BIZLINE,YEARMONTH, TYPE,CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL, VALUE			
					FROM ANALYSIS..FORECAST_2026_CALC
					WHERE (YEARMONTH LIKE '2026%'  OR YEARMONTH LIKE '2025%' )
					      AND TYPE IN ('BUDGET', 'FORECAST')
					) A
		PIVOT (SUM(VALUE) FOR TYPE IN ([FORECAST],[BUDGET])) PVT														

----------------------------------CALCULATION 1.6 Forecast 2026 / Budget 2026----------------------------------									

		INSERT INTO ANALYSIS..FORECAST_2026_CALC       									
		SELECT   BIZLINE,YEARMONTH
				,'FORECAST / BUDGET'							
			    ,CUS_TYPE,SEGMENT,PRODUCT_LV_4,PRODUCT_LV_3,PRODUCT_LV_2,PRODUCT_LV_1,MEASURE,LEVEL							
				,CASE WHEN [FORECAST]  <> 0 AND [BUDGET]  <> 0 THEN (ISNULL([FORECAST],0) - ISNULL([BUDGET],0))/ABS([BUDGET]) END					
				

		FROM (	SELECT BIZLINE,YEARMONTH, TYPE, CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL, VALUE			
					FROM ANALYSIS..FORECAST_2026_CALC
					WHERE (YEARMONTH LIKE '2026%'  OR YEARMONTH LIKE '2025%' )
					      AND TYPE IN ('BUDGET', 'FORECAST')
			 ) A
		PIVOT (SUM(VALUE) FOR TYPE IN ([FORECAST],[BUDGET])) PVT


	
----------------------------------Calculation 1.7 Forecast 2026 - Revised_Budget 2026----------------------------------					


		INSERT INTO ANALYSIS..FORECAST_2026_CALC       									
		SELECT   BIZLINE,YEARMONTH
				,'FORECAST - Revised_Budget'							
			    ,CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL							
				,CASE WHEN [FORECAST]  <> 0 AND [Revised_Budget]  <> 0 THEN ISNULL([FORECAST],0)-ISNULL([Revised_Budget],0) END			
				
		FROM (	SELECT BIZLINE,YEARMONTH, TYPE,CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL, VALUE			
					FROM ANALYSIS..FORECAST_2026_CALC
					WHERE (YEARMONTH LIKE '2026%'  OR YEARMONTH LIKE '2025%' )
					      AND TYPE IN ('Revised_Budget', 'FORECAST')
					) A
		PIVOT (SUM(VALUE) FOR TYPE IN ([FORECAST],[Revised_Budget])) PVT
														

--------------------------------CALCULATION 1.7 Forecast 2026 / Revised_Budget 2026----------------------------------									

		INSERT INTO ANALYSIS..FORECAST_2026_CALC       									
		SELECT   BIZLINE,YEARMONTH
				,'FORECAST / Revised_Budget'							
			    ,CUS_TYPE,SEGMENT,PRODUCT_LV_4,PRODUCT_LV_3,PRODUCT_LV_2,PRODUCT_LV_1,MEASURE,LEVEL							
				,CASE WHEN [FORECAST]  <> 0 AND [Revised_Budget]  <> 0 THEN (ISNULL([FORECAST],0) - ISNULL([Revised_Budget],0))/ABS([Revised_Budget]) END					
				

		FROM (	SELECT BIZLINE,YEARMONTH, TYPE, CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL, VALUE			
					FROM ANALYSIS..FORECAST_2026_CALC
					WHERE (YEARMONTH LIKE '2026%'  OR YEARMONTH LIKE '2025%' )
					      AND TYPE IN ('Revised_Budget', 'FORECAST')
			 ) A
		PIVOT (SUM(VALUE) FOR TYPE IN ([FORECAST],[Revised_Budget])) PVT


----------------------------------CALCULATION 1 .13 Forecast - Forecast LM && Forecast / Forecast LM----------------------------------									

		;
		WITH RAW AS 
			(SELECT BIZLINE, YEARMONTH, TYPE 
					,CUS_TYPE,SEGMENT,PRODUCT_LV_4,PRODUCT_LV_3,PRODUCT_LV_2,PRODUCT_LV_1,MEASURE,LEVEL,VALUE
			FROM ANALYSIS..FORECAST_2026_CALC
			WHERE YEARMONTH LIKE '2026%' AND TYPE IN ('Forecast', 'Forecast_LM')      --- 20260423: YTD
			)

		INSERT INTO  ANALYSIS..FORECAST_2026_CALC   

		SELECT BIZLINE, YEARMONTH
			 , 'Forecast - Forecast_LM' 
			 , CUS_TYPE,SEGMENT,PRODUCT_LV_4,PRODUCT_LV_3,PRODUCT_LV_2,PRODUCT_LV_1,MEASURE,LEVEL
			 , CASE WHEN [FORECAST_LM] <> 0 THEN ISNULL([FORECAST],0)-ISNULL([FORECAST_LM],0) END
		FROM RAW
		PIVOT (SUM(VALUE) FOR TYPE IN ([FORECAST],[FORECAST_LM])) PVT

		UNION ALL 

		SELECT BIZLINE, YEARMONTH 
			, 'Forecast / Forecast_LM' 
			, CUS_TYPE , SEGMENT,PRODUCT_LV_4 , PRODUCT_LV_3,PRODUCT_LV_2 , PRODUCT_LV_1 , MEASURE , LEVEL	
			, CASE WHEN [FORECAST_LM] <> 0 THEN (ISNULL([FORECAST],0) - ISNULL([FORECAST_LM],0))/ABS([FORECAST_LM]) END
		FROM RAW 
		PIVOT (SUM(VALUE) FOR TYPE IN ([FORECAST],[FORECAST_LM])) PVT
			

--================================PART 2 - SQL REPORT ============================									
									
/*									
-----BANG MAPPING DONG (TOA DO X)-------------------																		

SELECT * FROM ANALYSIS..REVISED_BUDGET_2026_X
ORDER BY ID ASC
;

SELECT * FROM ANALYSIS..REVISED_BUDGET_2026_X			
ORDER BY ID ASC

---------BANG MAPPING COT (TOA DO Y)-------------------									

SELECT * FROM Analysis..FC_2026_Y		ORDER BY ID ASC

SELECT * FROM Analysis..FC_2026_Y_SEG		ORDER BY ID ASC					

*/

-------OUTPUT THEO TEMPLATE REPORT TREN EXCEL	  --- PFS VIEW --------------------------												
	
Update Analysis..FC_2026_Y									
SET	Yearmonth=LEFT(Yearmonth,4)+RIGHT(@YTD_Month,2)+RIGHT(Yearmonth,4)						----- 20260423: YTD						
where Yearmonth like '%YTD' or Yearmonth like '%MTD'	

DECLARE @LIST_COLUMN_1 VARCHAR(MAX)																
DECLARE @iD_1 VARCHAR(20) ='1'									
								
									
SET @LIST_COLUMN_1 ='['+@iD_1+']'									
SET @iD_1 =@iD_1+1									
WHILE @iD_1 <= (SELECT MAX(ID) FROM Analysis..FC_2026_Y)									
BEGIN									
		SET @LIST_COLUMN_1 = @LIST_COLUMN_1+',['+@iD_1+']'									
		SET @iD_1 =@iD_1+1									
END									
									
PRINT @LIST_COLUMN_1									
											

EXEC('				
IF EXISTS ( SELECT * FROM ANALYSIS.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ''FORECAST_2026_PFS_VIEW_TOTAL'' )
DROP TABLE ANALYSIS..FORECAST_2026_PFS_VIEW_TOTAL								

SELECT ID_ROW,'+@LIST_COLUMN_1+' INTO ANALYSIS..FORECAST_2026_PFS_VIEW_TOTAL 										
FROM 									
	 (								
		SELECT X.ID AS ID_ROW,Y.ID AS ID_COLUMN, A.VALUE							
		  FROM ANALYSIS..FORECAST_2026_CALC A							
		  RIGHT JOIN ANALYSIS..REVISED_BUDGET_2026_X X							
				ON  A.PRODUCT_LV_4	=X.PRODUCT_LV_4							
				AND A.PRODUCT_LV_3	=X.PRODUCT_LV_3							
				AND A.PRODUCT_LV_2	=X.PRODUCT_LV_2							
				AND A.PRODUCT_LV_1	=X.PRODUCT_LV_1							
				AND A.LEVEL			=X.LEVEL							
				AND A.MEASURE		=X.MEASURE 							
				AND A.BIZLINE		=X.BIZLINE 							
		  LEFT JOIN Analysis..FC_2026_Y Y							
				ON  A.YEARMONTH		=Y.YEARMONTH							
				AND A.TYPE			=Y.TYPE							
				AND A.CUS_TYPE		=Y.CUS_TYPE							
				AND A.SEGMENT		=Y.SEGMENT	
	  ) AS A								
			  PIVOT(  SUM(VALUE) FOR ID_COLUMN IN ('+@LIST_COLUMN_1+')	 ) AS PVT									
ORDER BY ID_ROW ASC														

')														

PRINT 'EXCEL CALCULATION DONE'

--UPDATE ANALYSIS..BUDGET_2026_X
--SET Product_Lv_2 = 'CA'
--WHERE ID IN (176,177,178)

-------------- FORECAST_LOG--------
/*
DROP TABLE ANALYSIS..FORECAST_LOG

CREATE TABLE ANALYSIS..FORECAST_LOG
(TIME_CONSOL	DATETIME
,VERSION		VARCHAR(200)
,YEAR			VARCHAR(20)
,MEASURE		VARCHAR(20)
,VALUE			FLOAT
)
*/

--INSERT INTO ANALYSIS..FORECAST_LOG
--SELECT GETDATE() AS TIME_CONSOL, CONVERT(VARCHAR(10), GETDATE(), 112) AS VERSION,LEFT(MONTH,4) AS YEAR, 'TOI' AS MEASURE,SUM(VALUE) AS VALUE
--FROM ANALYSIS..FORECAST_2026_UNIONALL 
--WHERE MEASURE IN ('NII','NFI','TRADING INCOME','OTHER INCOME') AND LEFT(MONTH,4) = '2026'
--GROUP BY LEFT(MONTH,4)

--SELECT * FROM ANALYSIS..FORECAST_LOG
--ORDER BY TIME_CONSOL DESC

-- exec Analysis..SP_MONTHLY_FORECAST_PBI @START_FC, @END_FC




