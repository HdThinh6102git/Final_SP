CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_KBL`(
    IN IN_BATCH_ID       VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE  VARCHAR(20)
)
BEGIN
    -- [DECLARE variables]
    DECLARE v_raw_cols     TEXT         DEFAULT '';
    DECLARE v_proc_cols    TEXT         DEFAULT '';
    DECLARE v_sql_query    TEXT         DEFAULT '';
    DECLARE v_raw_table    VARCHAR(100) DEFAULT 'T_RPA_LIFE_RAW';
    DECLARE v_proc_table   VARCHAR(100) DEFAULT 'T_RPA_LIFE_PROCESSED';
    DECLARE v_row_count    INT          DEFAULT 0;
    DECLARE v_company_code VARCHAR(10)  DEFAULT 'KBL';
    DECLARE v_target_ym    VARCHAR(6)   DEFAULT '';
    DECLARE v_cutoff_ym    VARCHAR(6)   DEFAULT '';

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_KBL_PROCESSED;
    END;

    -- [SET logic]
    SET v_target_ym = DATE_FORMAT(NOW(), '%Y%m');
    -- Rule 5 cutoff: 38 months
    SET v_cutoff_ym = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 38 MONTH), '%Y%m');

    -- 1. Hardcoded Column Mapping for KB Life (KBL)
    -- 1. Hardcoded Column Mapping for KB Life (KBL)
    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        -- Mapping for NEW contracts (Columns 01-34 + Target-only 35)
        SET v_raw_cols = ''; SET v_proc_cols = '';
        
        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_01, ', -- 지점명
            'COLUMN_02, ', -- 수금설계사
            'COLUMN_03, '); -- 수금설계사코드
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_01, ', -- 지점명
            'COLUMN_02, ', -- 수금설계사
            'COLUMN_03, '); -- 수금설계사코드
        
        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_04, ', -- 증권번호
            'COLUMN_05, ', -- 계약자
            'COLUMN_06, '); -- 피보험자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_04, ', -- 증권번호
            'COLUMN_05, ', -- 계약자
            'COLUMN_06, '); -- 피보험자
        
        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_07, ', -- 상품
            'COLUMN_08, ', -- 상품코드
            'COLUMN_09, '); -- 계약일자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_07, ', -- 상품
            'COLUMN_08, ', -- 상품코드
            'COLUMN_09, '); -- 계약일자
        
        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_10, ', -- 상태
            'COLUMN_11, ', -- 계약상태
            'COLUMN_12, '); -- 보험료
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_10, ', -- 상태
            'COLUMN_11, ', -- 계약상태
            'COLUMN_12, '); -- 보험료
        
        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_13, ', -- 실납입보험료
            'COLUMN_14, ', -- 보험료(달러)
            'COLUMN_15, '); -- 선수/선납보험료
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_13, ', -- 실납입보험료
            'COLUMN_14, ', -- 보험료(달러)
            'COLUMN_15, '); -- 선수/선납보험료
        
        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_16, ', -- 최종납입월
            'COLUMN_17, ', -- 최종납입월(예정)
            'COLUMN_18, '); -- 최종횟수
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_16, ', -- 최종납입월
            'COLUMN_17, ', -- 최종납입월(예정)
            'COLUMN_18, '); -- 최종횟수
        
        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_19, ', -- 최종횟수(예정)
            'COLUMN_20, ', -- 최종납입일
            'COLUMN_21, '); -- 차기납입일자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_19, ', -- 최종횟수(예정)
            'COLUMN_20, ', -- 최종납입일
            'COLUMN_21, '); -- 차기납입일자
        
        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_22, ', -- 수금방법
            'COLUMN_23, ', -- 납입방법
            'COLUMN_24, '); -- 이체일
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_22, ', -- 수금방법
            'COLUMN_23, ', -- 납입방법
            'COLUMN_24, '); -- 이체일
        
        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_25, ', -- 납입기간
            'COLUMN_26, ', -- 계약상태변경일
            'COLUMN_27, '); -- 만기/소멸일
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_25, ', -- 납입기간
            'COLUMN_26, ', -- 계약상태변경일
            'COLUMN_27, '); -- 만기/소멸일
        
        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_28, ', -- 확정회차
            'COLUMN_29, ', -- 모집설계사
            'COLUMN_30, '); -- 모집설계사코드
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_28, ', -- 확정회차
            'COLUMN_29, ', -- 모집설계사
            'COLUMN_30, '); -- 모집설계사코드
        
        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_31, ', -- 가입금액
            'COLUMN_32, ', -- 환산성적(초년도)
            'COLUMN_33, '); -- 환산성적(2차년도)
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_31, ', -- 가입금액
            'COLUMN_32, ', -- 환산성적(초년도)
            'COLUMN_33, '); -- 환산성적(2차년도)
        
        -- 34-35
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_34, ', -- 환산성적(3차년도)
            'NULL'); -- 납기구분
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_34, ', -- 환산성적(3차년도)
            'COLUMN_35'); -- 납기구분

    ELSEIF UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN
        -- Mapping for EXT contracts (Columns 01-34)
        SET v_raw_cols = ''; SET v_proc_cols = '';
        
        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_01, ', -- 지점명
            'COLUMN_02, ', -- 수금설계사
            'COLUMN_03, '); -- 수금설계사코드
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_01, ', -- 지점명
            'COLUMN_02, ', -- 수금설계사
            'COLUMN_03, '); -- 수금설계사코드
        
        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_04, ', -- 증권번호
            'COLUMN_05, ', -- 계약자
            'COLUMN_06, '); -- 피보험자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_04, ', -- 증권번호
            'COLUMN_05, ', -- 계약자
            'COLUMN_06, '); -- 피보험자
        
        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_07, ', -- 상품
            'COLUMN_08, ', -- 상품코드
            'COLUMN_09, '); -- 계약일자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_07, ', -- 상품
            'COLUMN_08, ', -- 상품코드
            'COLUMN_09, '); -- 계약일자
        
        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_10, ', -- 상태
            'COLUMN_11, ', -- 계약상태
            'COLUMN_12, '); -- 보험료
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_10, ', -- 상태
            'COLUMN_11, ', -- 계약상태
            'COLUMN_12, '); -- 보험료
        
        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_13, ', -- 실납입보험료
            'COLUMN_14, ', -- 보험료(달러)
            'COLUMN_15, '); -- 선수/선납보험료
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_13, ', -- 실납입보험료
            'COLUMN_14, ', -- 보험료(달러)
            'COLUMN_15, '); -- 선수/선납보험료
        
        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_16, ', -- 최종납입월
            'COLUMN_17, ', -- 최종납입월(예정)
            'COLUMN_18, '); -- 최종횟수
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_16, ', -- 최종납입월
            'COLUMN_17, ', -- 최종납입월(예정)
            'COLUMN_18, '); -- 최종횟수
        
        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_19, ', -- 최종횟수(예정)
            'COLUMN_20, ', -- 최종납입일
            'COLUMN_21, '); -- 차기납입일자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_19, ', -- 최종횟수(예정)
            'COLUMN_20, ', -- 최종납입일
            'COLUMN_21, '); -- 차기납입일자
        
        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_22, ', -- 수금방법
            'COLUMN_23, ', -- 납입방법
            'COLUMN_24, '); -- 이체일
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_22, ', -- 수금방법
            'COLUMN_23, ', -- 납입방법
            'COLUMN_24, '); -- 이체일
        
        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_25, ', -- 납입기간
            'COLUMN_26, ', -- 계약상태변경일
            'COLUMN_27, '); -- 만기/소멸일
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_25, ', -- 납입기간
            'COLUMN_26, ', -- 계약상태변경일
            'COLUMN_27, '); -- 만기/소멸일
        
        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_28, ', -- 확정회차
            'COLUMN_29, ', -- 모집설계사
            'COLUMN_30, '); -- 모집설계사코드
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_28, ', -- 확정회차
            'COLUMN_29, ', -- 모집설계사
            'COLUMN_30, '); -- 모집설계사코드
        
        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_31, ', -- 가입금액
            'COLUMN_32, ', -- 환산성적(초년도)
            'COLUMN_33, '); -- 환산성적(2차년도)
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_31, ', -- 가입금액
            'COLUMN_32, ', -- 환산성적(초년도)
            'COLUMN_33, '); -- 환산성적(2차년도)
        
        -- 34
        SET v_raw_cols = CONCAT(v_raw_cols, 'COLUMN_34'); -- 환산성적(3차년도)
        SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_34'); -- 환산성적(3차년도)
    END IF;


    -- 2. Build sql query insert temp table
    IF v_raw_cols != '' AND v_proc_cols != '' THEN
        
        -- Create Temporary Table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_KBL_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_KBL_PROCESSED LIKE ', v_proc_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        -- Insert into Temporary Table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_KBL_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''KBL'', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''KBL'' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''') ',
            '  AND COLUMN_04 <> ''증권번호'';'
        );
        
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- 3. Apply Transformation Logic
        
        IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            -- [Rule 2] 항목명 : 납기구분 / 항목값 : 년납
            UPDATE T_TEMP_RPA_KBL_PROCESSED SET COLUMN_35 = '년납';

        ELSEIF UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN
            -- [Rule 1] 계약상태변경일 편집
            UPDATE T_TEMP_RPA_KBL_PROCESSED SET COLUMN_26 = '0000-00-00'
            WHERE COLUMN_10 IN ('계류', '정상');

            UPDATE T_TEMP_RPA_KBL_PROCESSED SET COLUMN_26 = '0000-00-00'
            WHERE COLUMN_10 = '종료' AND COLUMN_11 IN ('실효(환급금없는실효)', '실효(환급금있는실효)');

            -- [Rule 2.1] [최종납입월](16) 빈셀 or 1900-01 & [계약상태]="계류(성립이전),반송,철회"면 [최종납입월]=계약년월
            UPDATE T_TEMP_RPA_KBL_PROCESSED SET COLUMN_16 = LEFT(COLUMN_09, 7)
            WHERE (COLUMN_16 IS NULL OR COLUMN_16 = '' OR COLUMN_16 = '1900-01')
              AND COLUMN_11 IN ('계류(성립이전)', '반송', '철회');

            -- [Rule 2.3] [납입방법](23) 빈셀이면 -> [최종횟수](18)="1"이면 [최종납입일](20)=계약일자, 아니면 0000-00-00
            UPDATE T_TEMP_RPA_KBL_PROCESSED 
            SET COLUMN_20 = CASE WHEN COLUMN_18 = '1' THEN COLUMN_09 ELSE '0000-00-00' END
            WHERE COLUMN_23 IS NULL OR COLUMN_23 = '';

            -- [Rule 3] [납입방법](23)="일시납"이면 (1) [보험료](12)="0" (2) [최종횟수](18)="1"
            UPDATE T_TEMP_RPA_KBL_PROCESSED SET COLUMN_12 = '0', COLUMN_18 = '1' 
            WHERE COLUMN_23 = '일시납';

            -- [Rule 4] [계약상태]="신계약" & [최종납입월] (yyyy-mm) =2개월전 -> [계약상태]="실효".
            UPDATE T_TEMP_RPA_KBL_PROCESSED SET COLUMN_11 = '실효'
            WHERE COLUMN_11 = '신계약'
              AND REPLACE(COLUMN_16, '-', '') = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 2 MONTH), '%Y%m');

            -- [Rule 5] [계약상태]="실효" & 실효 3년 경과(38개월) -> [계약상태]="시효"
            UPDATE T_TEMP_RPA_KBL_PROCESSED
            SET COLUMN_11 = '시효'
            WHERE COLUMN_11 IN ('실효(환급금없는실효)', '실효(환급금있는실효)')
              AND COLUMN_16 IS NOT NULL
              AND REPLACE(SUBSTRING(COLUMN_16, 1, 7), '-', '') <= v_cutoff_ym;
        END IF;

        -- 4. Build sql query insert processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_proc_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_KBL_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();

        -- 5. Drop temporary table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_KBL_PROCESSED;

    END IF;

END