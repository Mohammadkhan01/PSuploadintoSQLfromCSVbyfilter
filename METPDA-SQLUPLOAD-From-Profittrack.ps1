
#This function will oepn xml file and convert it in a sql table format and snatch filed which is match with xml and sql table (with some requirement)
Function createDboProductCSV{
    [xml]$ptXMLData = Get-Content $ptInfputFile
    $objects = $ptXMLData.DATAPACKET.ROWDATA.ChildNodes
    $matchcounter = 0
    $start = [system.datetime]::Now
    foreach($object in $objects)
    {
         $obj="" | select "product_id","productName","subrange","subrangeName",@{Name ='department';Expression = {$_.department -as [int]}},"departmentName",@{Name ='category';Expression = {$_.category -as [int]}},"categoryName",@{Name ='commodity';Expression = {$_.commodity -as [int]}},"commodityName",@{Name ='subcommodity';Expression = {$_.subcommodity -as [int]}},"subCommodityName",@{Name ='averageDailyQuantity';Expression = {$_.averageDailyQuantity -as [Double]}},
         @{Name ='quantityonhand';Expression={[convert]::ToDouble($_.quantityonhand)}}, @{Name ='quantityonorder';Expression = {$_.quantityonorder -as [Double]}},"gtinPLU","productMultiple",@{Name="isActive";expression={$_.isActive -as [Boolean]}},@{Name='currentRetai';Expression={[convert]::ToDouble($_.currentRetai)}},"supplier","supplierCode", @{Name='cartonQuantity';Expression={[convert]::ToDouble($_.cartonQuantity)}},@{Name='minOrdQuantity';Expression={[convert]::ToDouble($_.minOrdQuantity)}}
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
            [int]$obj.'quantityOnHand'=$object.'STOCKONHAND'
            $obj.'quantityOnOrder'=0.0
            $obj.'gtinPLU'=$object.'PLU'
            $obj.'productMultiple'=0.0
            $start1 = [datetime][system.datetime]::Now
          
            If($object.'LASTSALEDATE' -ne ""){
            $end1 = get-date $object.'LASTSALEDATE'
                if(($start1-$end1).TotalDays -lt 90){           
                    [boolean]$obj.'isActive'=1
                } else{
                [boolean]$obj.'isActive'=0
                }
            } else {
                [boolean]$obj.'isActive'=0
            }

            if($object.'SPECIALSELL' -ne ""){
                if($object.'SPECIALSELL'>0){
                    [double]$obj.'currentRetai'=$object.'SPECIALSELL'
                } else {
                    [double]$obj.'currentRetai'= 0.0
                }
            } else {
                if($object.'NORMSELL' > 0){
                    [double]$obj.'currentRetai'=$object.'NORMSELL'
                } else {
                    [double]$obj.'currentRetai'= 0.0
                }
            }
            $obj.'supplier'=""
            $obj.'suppliercode'=$object.'PRODUCTCODE'
            [double]$obj.'cartonQuantity'=0.0
            [double]$obj.'minOrdQuantity'=0.0
            if($matchcounter -gt 1000){ break;}
            $obj |Export-Csv -Path  $script:Path\$script:tempFile -Append
    }
        $end = [system.datetime]::Now
        $resultTime = $end - $start
    Write-Host "Execution took : $($resultTime.TotalSeconds) seconds------------------------------------------------------------------------."
}




#This function will update sql database from a csv file (line by line) which we have created from excel file.
function Do-FilesInsertRowByRow ([Data.SqlClient.SqlConnection] $OpenSQLConnection) {

 
        $files = Import-CSV -Path $script:Path\$script:tempFile
        $sqlCommand = New-Object System.Data.SqlClient.SqlCommand
        $sqlCommand.Connection = $sqlConnection
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
        $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@isActive",[Data.SQLDBType]::bit))) | Out-Null
        $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@currentRetail",[Data.SQLDBType]::decimal))) | Out-Null
        $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@supplier",[Data.SQLDBType]::NvarChar,260))) | Out-Null
        $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@supplierCode",[Data.SQLDBType]::NvarChar,260))) | Out-Null
        $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@cartonQuantity",[Data.SQLDBType]::decimal))) | Out-Null
        $sqlCommand.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@minOrdQuantity",[Data.SQLDBType]::decimal))) | Out-Null
        $counter = 0
        foreach ($file in $files) {
            $counter++
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
    write-host "Total Number of rows inserted :"+ $counter
}

function Do-Filesdelete ([Data.SqlClient.SqlConnection] $OpenSQLConnection) {

        $sqlConnection1 = New-Object System.Data.SqlClient.SqlConnection
        $sqlConnection1.ConnectionString = "Server=$DBServer;Database=$DBName;Integrated Security=True;"
        $sqlConnection1.Open()
       
        $sqlCommand1 = New-Object System.Data.SqlClient.SqlCommand
        $sqlCommand1.Connection = $sqlConnection1
        $sqlCommand1.CommandText = "SET NOCOUNT ON; " +"delete from dbo.Product" 

        $sqlCommand1.ExecuteScalar()
         if ($sqlConnection1.State -eq [Data.ConnectionState]::Open) {
            $sqlConnection1.Close()
        }
}

#================================================================
#create a temp directory
$script:Path = "C:\METPDA"
$computerName=$env:COMPUTERNAME
$script:tempFile = 'isProductExport.csv'
$DBServer = "$computerName\SQLEXPRESS"
$DBName = "METPDA"
$ptInfputFile= 'C:\MetPDA\Script\Products.xml'
#Checking existing file and delete

#=================================================================
IF(Test-path $Path\$tempFile){
		    Remove-Item -Force  $Path\$tempFile
}
#=================================================================
       Do-Filesdelete($sqlConnection) #Remove Data from SQL
#=================================================================
$sqlConnection = New-Object System.Data.SqlClient.SqlConnection
$sqlConnection.ConnectionString = "Server=$DBServer;Database=$DBName;Integrated Security=True;"
$sqlConnection.Open()
if ($sqlConnection.State -ne [Data.ConnectionState]::Open) {
    "Connection to DB is not open."
     Exit
}
#=============Main Function======================================
        
       
createDboProductCSV

Do-FilesInsertRowByRow ($sqlConnection)
# Close the connection.

 if ($sqlConnection.State -eq [Data.ConnectionState]::Open) {
    $sqlConnection.Close()
}
Write-Host "Finished." 
# ===========================script:=========================================script:	# End
# ================================================================================
