function Decode-Hex($hex) {
    if ($hex -eq "NULL" -or -not $hex) { return "" }
    try {
        $bytes = for($i=0; $i -lt $hex.Length; $i+=2) { [Convert]::ToByte($hex.Substring($i, 2), 16) }
        return [System.Text.Encoding]::UTF8.GetString($bytes)
    } catch {
        return "[$hex]"
    }
}

$companies = @('SSL', 'LNF', 'DBL', 'MTL', 'KBL', 'CDL', 'SSF', 'MRF', 'HDG', 'DBG', 'KBG', 'LTG')
$outputFile = 'c:\Users\PC\Downloads\Final_SP\decoded_mappings.txt'
"--- Final Mappings ---" | Out-File -FilePath $outputFile -Encoding utf8

foreach ($co in $companies) {
    "`n--- $co ---" | Out-File -FilePath $outputFile -Encoding utf8 -Append
    foreach ($ct in @('NEW', 'EXT')) {
        "`n[$ct Mapping]" | Out-File -FilePath $outputFile -Encoding utf8 -Append
        $query = "SELECT RAW_COLUMN_NAME, HEX(PROCESSED_SEMANTIC_NAME) FROM rpa_insurance.T_RPA_RAW_META WHERE COMPANY_CODE = '$co' AND CONTRACT_TYPE = '$ct' ORDER BY ABS(REPLACE(RAW_COLUMN_NAME,'COLUMN_',''))"
        $rows = mysql -u root -pmysql123@ --default-character-set=utf8mb4 -N -s -e $query
        
        foreach ($row in $rows) {
            # Split by whitespace (tab or space)
            $parts = $row -split "\s+"
            if ($parts.Count -eq 2) {
                $col = $parts[0]
                $hex = $parts[1]
                $name = Decode-Hex $hex
                "SET v_raw_cols = CONCAT(v_raw_cols, '$col, '); -- $name" | Out-File -FilePath $outputFile -Encoding utf8 -Append
                "SET v_proc_cols = CONCAT(v_proc_cols, '$col, '); -- $name" | Out-File -FilePath $outputFile -Encoding utf8 -Append
            }
        }
    }
}
