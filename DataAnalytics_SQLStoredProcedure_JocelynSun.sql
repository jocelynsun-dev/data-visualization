USE [ReportingDB]
GO
/****** Object:  StoredProcedure [dbo].[StoredProcedure_Demo_PaymentDetail]    Script Date: 2/1/2026 10:06:07 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER OFF
GO

ALTER PROCEDURE [dbo].[StoredProcedure_Demo_PaymentDetail]

@VendorPaymentCheckBeginDate datetime
,@VendorPaymentCheckEndDate	datetime--nvarchar(10) 

/*
Name:		StoredProcedure_Demo_PaymentDetail
Purpose:	To generate a transaction detail report
Author:		JOCELYN SUN
Notes:		**For Demo purposes only**
CreateDate: 2026-02-01
Location: //
ModifiedDate: 

[StoredProcedure_Demo_PaymentDetail]  @VendorPaymentCheckBeginDate='2026-01-01', @VendorPaymentCheckEndDate='2026-02-01'

*/

AS
BEGIN

IF OBJECT_ID('TempDB..##PaymentDetail_All') IS NOT NULL
    DROP TABLE ##PaymentDetail_All

CREATE TABLE ##PaymentDetail_All (
	ServerName NVARCHAR(20),
	DatabaseName NVARCHAR(20),
	CustomerNumber NVARCHAR(100),
	CompanyName NVARCHAR(1000),
    InvoiceID NVARCHAR(500),
	TransactionID NVARCHAR(100),
    TotalAmount DECIMAL(20,2),
    Date CHAR(10)  --YearMonth CHAR(7)
);


DECLARE @ShortName NVARCHAR(50) =NULL,
		@Srv NVARCHAR(128) =NULL,
		@DB NVARCHAR(128) =NULL,
		@sql NVARCHAR(MAX)


DECLARE db_cursor CURSOR FOR
    SELECT fShortName 
    FROM [Listener-SQL000].[Accknowledge Authentication].dbo.tCustomer  -- all customers
	--WHERE fShortName='CustomerABC'


OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @ShortName;

WHILE @@FETCH_STATUS = 0
BEGIN
    ---- Get server and DB name for this customer
    SELECT @Srv = RTRIM(fAccDBServer),
           @DB  = RTRIM(fAccDBName)
    FROM [SQLServer101].[Authentication].dbo.tCustomer 
    WHERE fShortName = @ShortName
	AND fAccDBServer IN ('customer1','customer2','customer3') 
	;

    ---- Build query dynamically against that database
    SET @sql = 
	'
	INSERT INTO ##PaymentDetail_All (ServerName, DatabaseName, CustomerNumber, CompanyName, InvoiceID, TransactionID, TotalAmount, Date) 

    SELECT 
		detail.ServerName,
		detail.DatabaseName,
		detail.CustomerNumber,
		detail.CompanyName,
		detail.InvoiceID,
		detail.TransactionID,
        Sum(detail.Amount) as TotalAmount,
		detail.Date

	 FROM (
	     SELECT 
		'''+ @Srv +''' AS ServerName,
		'''+ @DB +''' AS DatabaseName,
		'''+ @ShortName +''' + ''-'' + LEFT(c.fID,3) + ''-'' + LEFT(pr.fID,3) AS CustomerNumber,
		c.fName as CompanyName,
		p.fInvoiceID AS InvoiceID,
		p.fEPTransactionID AS TransactionID,
        p.fAmount AS Amount,
		CAST(p.fDateAdded AS date) AS Date
        FROM [' + @Srv + '].[' + @DB + '].dbo.tAPVendor AS v   
		inner join [' + @Srv + '].[' + @DB + '].dbo.tAPInvoice as i on v.fVendorID = i.fVendorID 
		inner join [' + @Srv + '].[' + @DB + '].dbo.tAPPayment as p on i.fInvoiceID = p.fInvoiceID
        inner join [' + @Srv + '].[' + @DB + '].dbo.tSCCompany AS c ON c.fCompanyID = v.fCompanyID  
		left join [' + @Srv + '].[' + @DB + '].dbo.tSCProperty AS pr on v.fcompanyid= pr.fcompanyid
		left join [Listener-SQL000].[services].dbo.tEPayTransactions as pt on pt.fEPayTransactionID = p.fEPayTransactionID
		left join [' + @Srv + '].[' + @DB + '].dbo.tSCEpayTypes as ept on ept.fEpayEnum =pt.fEPayVendor
		WHERE CAST(p.fDateAdded AS date) >= ''' + Convert(nvarchar(10), @VendorPaymentCheckBeginDate,121) + '''   AND CAST(p.fDateAdded AS date) < ''' + Convert(nvarchar(10), @VendorPaymentCheckEndDate,121) + ''' 
        AND p.fVoid = 0
		) as detail
		GROUP BY
		detail.ServerName,
		detail.DatabaseName,
		detail.CustomerNumber,
		detail.CompanyName,
		detail.InvoiceID,
		detail.TransactionID,
		detail.Date
	'
;

---- Debug: see generated SQL
PRINT @sql; 

EXEC sp_executesql @sql;

FETCH NEXT FROM db_cursor INTO @ShortName;
END

CLOSE db_cursor;
DEALLOCATE db_cursor;

----save dataset to a csv file on sftp directory 
Declare @FileName varchar(500) = 'PaymentDetails' + Left(DATENAME(month, CAST(@targetmonth AS DATE)),3) + '-' + Cast(Format(Day(EOMONTH(@targetMonth)),'00') as varchar(2)) + '.csv'		
Declare @sftpDirectory varchar(500) = '\\sftpdirectory\VendorPayment\'
Set @FileName = @sftpDirectory + @FileName
Select @FileName

Declare @bcpCommand varchar(1000), @ExportFileName varchar(512), @FileDate varchar(32)

set @bcpCommand = 'bcp "select ''ServerName'', ''DatabaseName'', ''CustomerNumber'', ''CompanyName'', ''InvoiceID'', ''TransactionID'', ''TotalAmount'', ''Date'' union all Select ServerName, DatabaseName, CustomerNumber, CompanyName, InvoiceID, TransactionID, Cast(TotalAmount as nvarchar(20)), Cast(Date as nvarchar(10))  from M3_DNA.dbo.PaymentDetail" queryout ' + @FileName + ' -c -t, -T'

print @bcpCommand
exec master..xp_cmdshell @bcpCommand


END;