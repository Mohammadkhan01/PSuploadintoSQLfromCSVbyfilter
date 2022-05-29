

#create a temp directory
$Path = "C:\Temp"

$tempFile = 'isProductExport.csv'
$ptInfputFile= 'C:\Temp\products.xml'
$DBServer = "MP-SUP-100221\SQLEXPRESS"
$databasename = "METPDA"


Function createDboProductCSV{

    [xml]$ptXMLData = Get-Content $ptInfputFile
   
    $objects = $ptXMLData.DATAPACKET.ROWDATA.ChildNodes
    
    $matchcounter = 0
    $start = [system.datetime]::Now
    foreach($object in $objects)
    {
         $obj="" | select "product_id","productName","subrange","subrangeName",@{Name ='department';Expression = {$_.department -as [int]}},"departmentName",@{Name ='category';Expression = {$_.category -as [int]}},"categoryName",@{Name ='commodity';Expression = {$_.commodity -as [int]}},"commodityName",@{Name ='subcommodity';Expression = {$_.subcommodity -as [int]}},"subCommodityName",@{Name ='averageDailyQuantity';Expression = {$_.averageDailyQuantity -as [Double]}},
         @{Name ='quantityonhand';Expression = {$_.quantityonhand -as [Double]}}, @{Name ='quantityonorder';Expression = {$_.quantityonorder -as [Double]}},"gtinPLU","productMultiple",@{Name="isActive";Expression={[convert]::ToBoolean($_.isActive)}},"currentRetai","supplier","supplierCode", "cartonQuantity","minOrdQuantity"
          Write-Progress -activity "Writing data in CSV file . ." -status "Table Format from PT for DBO.Product table: %  :  $($matchcounter)" -percentComplete (($matchcounter / $objects.count)  * 100) 
            $matchCounter++
          
            $obj.'product_id' = $matchcounter
            $obj.'productName' = $object.'DESCRIPTION'
            $obj.'subrange' = ""
            $obj.'subrangeName'=""
            [int]$obj.'department' = ($object.'MSCDEPARTMENTNUMBER')
            $obj.'departmentName' = $object.'MSCDEPARTMENTNAME'
            [int]$obj.'category'=$object.'MSCCATEGORYNUMBER'
            $obj.'categoryName'=$object.'MSCCATEGORYNAME'
            [int]$obj.'commodity'=$object.'MSCCOMMODITYNUMBER'
            $obj.'commodityName'=$object.'MSCCOMMODITYNAME'
            [int]$obj.'subcommodity'=$object.'MSCSUBCOMMODITYNUMBER'
            $obj.'subCommodityName'=$object.'MSCSUBCOMMODITYNAME'
            $obj.'averageDailyQuantity'=0.0
            $obj.'quantityOnHand'=$object.'STOCKONHAND'
            $obj.'quantityOnOrder'=0.0
            $obj.'gtinPLU'=$object.'PLU'
            $obj.'productMultiple'=0.0
            $obj.'isActive'=0
            $obj.'currentRetai'=$object.'NORMSELL'
            $obj.'supplier'=""
            $obj.'suppliercode'=$object.'PRODUCTCODE'
            $obj.'cartonQuantity'=0.0
            $obj.'minOrdQuantity'=0.0
           if($matchcounter -gt 100){
            break;}
      
            $obj |Export-Csv -Path C:\Temp\$tempFile -Append
    }
        $end = [system.datetime]::Now
        $resultTime = $end - $start
        #$objects=""
        #$object=""
    Write-Host "Execution took : $($resultTime.TotalSeconds) seconds------------------------------------------------------------------------."
}

#$dtable = "C:\Temp\isProductExport.csv"  
#$cn = new-object System.Data.SqlClient.SqlConnection("Data Source=$dbserver;Integrated Security=SSPI;Initial Catalog=$databasename");
#$cn.Open()
#$bc = new-object ("System.Data.SqlClient.SqlBulkCopy") $cn
#$bc.DestinationTableName = "dbo.Product"
#$bc.WriteToServer($dtable)
#$cn.Close()





function BulkInsertDataToSql{
   
 	
Import-CSV -Path C:\Temp\isProductExport.csv | export-clixml c:\temp\users.xml   

$DBServer = "MP-SUP-100221\SQLEXPRESS"
$databasename = "METPDA"
$Connection = new-object system.data.sqlclient.sqlconnection #Set new object to connect to sql database
$Connection.ConnectionString ="server=$DBServer;database=$databasename;trusted_connection=True" # Connectiongstring setting for local machine database with window authentication
Write-host "Connection Information:"  -foregroundcolor yellow -backgroundcolor black
$Connection #List connection information
$SqlCmd = New-Object System.Data.SqlClient.SqlCommand #setting object to use sql commands

$SqlQuery = "BULK INSERT Product FROM 'C:\Temp\isProductExport.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n',
	  FIRSTROW = 3
)
"

$Connection.open()
Write-host "Connection to database successful." -foregroundcolor green -backgroundcolor black
$SqlCmd.CommandText = $SqlQuery
$SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
$SqlAdapter.SelectCommand = $SqlCmd
$SqlCmd.Connection = $Connection
$DataSet = New-Object System.Data.DataSet
$SqlAdapter.Fill($DataSet)
$Connection.Close()
#$DataSet.Tables[0]
}


#getdata

#BulkInsertDataToSql


function Do-FilesInsertRowByRow ([Data.SqlClient.SqlConnection] $OpenSQLConnection) {

 
 $files = Import-CSV -Path C:\Temp\isProductExport.csv
    $sqlCommand = New-Object System.Data.SqlClient.SqlCommand

    $sqlCommand.Connection = $sqlConnection
    # 

    $sqlCommand.CommandText = "SET NOCOUNT ON; " +

        "INSERT INTO dbo.product (productid,productName,subrange,subrangeName,department,departmentName,category,categoryName,commodity,commodityName,subcommodity,subCommodityName,averageDailyQuantity,quantityOnHand,quantityOnOrder,gtinPLU,productMultiple,isActive,currentRetail,supplier,supplierCode,cartonQuantity,minOrdQuantity) " +

        "VALUES (@productid,@productName,@subrange,@subrangeName,@department,@departmentName,@category,@categoryName,@commodity,@commodityName,@subcommodity,@subCommodityName,@averageDailyQuantity,@quantityOnHand,@quantityOnOrder,@gtinPLU,@productMultiple,@isActive,@currentRetail,@supplier,@supplierCode,@cartonQuantity,@minOrdQuantity); " 

    $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@productid",[Data.SQLDBType]::NVarChar, 32))) | Out-Null

    $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@productname",[Data.SQLDBType]::NvarChar,260))) | Out-Null

    $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@subrange",[Data.SQLDBType]::NVarChar, 260))) | Out-Null

    $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@subrangeName",[Data.SQLDBType]::NvarChar,260))) | Out-Null


$sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@department",[Data.SQLDBType]::BigInt))) | Out-Null

    $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@departmentName",[Data.SQLDBType]::NvarChar,260))) | Out-Null

$sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@category",[Data.SQLDBType]::BigInt))) | Out-Null

    $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@categoryName",[Data.SQLDBType]::NvarChar,260))) | Out-Null
$sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@commodity",[Data.SQLDBType]::BigInt))) | Out-Null

    $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@commodityName",[Data.SQLDBType]::NvarChar,260))) | Out-Null
$sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@subcommodity",[Data.SQLDBType]::BigInt))) | Out-Null

    $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@subCommodityName",[Data.SQLDBType]::NvarChar,260))) | Out-Null
$sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@averageDailyQuantity",[Data.SQLDBType]::decimal))) | Out-Null

   $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@quantityOnHand",[Data.SQLDBType]::decimal))) | Out-Null
   $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@quantityOnOrder",[Data.SQLDBType]::decimal))) | Out-Null

    $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@gtinPlu",[Data.SQLDBType]::NvarChar,260))) | Out-Null
$sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@productMultiple",[Data.SQLDBType]::Decimal))) | Out-Null

    $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@isActive",[Data.SQLDBType]::boolean))) | Out-Null
$sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@currentRetail",[Data.SQLDBType]::decimal))) | Out-Null

    $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@supplier",[Data.SQLDBType]::NvarChar,260))) | Out-Null
$sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@supplierCode",[Data.SQLDBType]::Decimal))) | Out-Null

    
   $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@cartonQuantity",[Data.SQLDBType]::decimal))) | Out-Null
   $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@minOrdQuantity",[Data.SQLDBType]::decimal))) | Out-Null

    # I love how I can foreach over a call like "dir *.*" in PowerShell!!

    foreach ($file in $files) {

        # Here we set the values of the pre-existing parameters based on the $file iterator

        $sqlCommand.Parameters[0].Value = $file.product_id

        $sqlCommand.Parameters[1].Value = $file.productName

        $sqlCommand.Parameters[2].Value = $file.subrange

         $sqlCommand.Parameters[3].Value = $file.subrangename

        $sqlCommand.Parameters[4].Value = $file.department
        
         $sqlCommand.Parameters[5].Value = $file.departmentName

        $sqlCommand.Parameters[6].Value = $file.category
         $sqlCommand.Parameters[7].Value = $file.categoryName

        $sqlCommand.Parameters[8].Value = $file.commodity 
        $sqlCommand.Parameters[9].Value = $file.commodityName

        $sqlCommand.Parameters[10].Value = $file.subcommodity
        $sqlCommand.Parameters[11].Value = $file.subcommodityname

        $sqlCommand.Parameters[12].Value = $file.averageDailyQuantity 
         $sqlCommand.Parameters[13].Value = $file.quantityonhand

        $sqlCommand.Parameters[14].Value = $file.quantityonorder
        $sqlCommand.Parameters[15].Value = $file.gtinPLU 
         $sqlCommand.Parameters[16].Value = $file.productMultiple

        $sqlCommand.Parameters[17].Value = $file.isActive
         $sqlCommand.Parameters[18].Value = $file.currentRetai
        $sqlCommand.Parameters[19].Value = $file.supplier 
         $sqlCommand.Parameters[20].Value = $file.supplierCode

        $sqlCommand.Parameters[21].Value = $file.cartonQuantity
        $sqlCommand.Parameters[22].Value=$file.minOrdQuantity





        $InsertedID = $sqlCommand.ExecuteScalar()
    }
}


# Open SQL connection (you have to change these variables)




$DBServer = "MP-SUP-100221\SQLEXPRESS"
$DBName = "METPDA"

$sqlConnection = New-Object System.Data.SqlClient.SqlConnection

$sqlConnection.ConnectionString = "Server=$DBServer;Database=$DBName;Integrated Security=True;"

$sqlConnection.Open()

 

# Quit if the SQL connection didn't open properly.

if ($sqlConnection.State -ne [Data.ConnectionState]::Open) {

    "Connection to DB is not open."

    Exit

}


 

# Call the function that does the inserts.
createDboProductCSV
Do-FilesInsertRowByRow ($sqlConnection)

 

# Close the connection.

if ($sqlConnection.State -eq [Data.ConnectionState]::Open) {

    $sqlConnection.Close()

}