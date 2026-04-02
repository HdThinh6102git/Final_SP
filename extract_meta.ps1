$companies = @('SSL', 'LNF', 'DBL', 'MTL', 'KBL', 'CDL', 'SSF', 'MRF', 'HDG', 'DBG', 'KBG', 'LTG')
$outputFile = 'c:\Users\PC\Downloads\Final_SP\all_mappings_final.txt'
$header = "COMPANY`tCONTRACT`tTYPE`tRAW_COL`tSEMANTIC"
$header | Out-File -FilePath $outputFile -Encoding utf8

foreach ($co in $companies) {
    $query = "SELECT COMPANY_CODE, CONTRACT_TYPE, INSURANCE_TYPE, RAW_COLUMN_NAME, PROCESSED_SEMANTIC_NAME FROM rpa_insurance.T_RPA_RAW_META WHERE COMPANY_CODE = '$co' ORDER BY CONTRACT_TYPE, RAW_COLUMN_NAME"
    mysql -u root -pmysql123@ --default-character-set=utf8mb4 -N -s -e $query | Out-File -FilePath $outputFile -Encoding utf8 -Append
}
