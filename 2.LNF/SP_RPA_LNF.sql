/*
 * SP_RPA_LNF
 * Description : Process Lina Life insurance data
 * Parameters  :
 *   IN_BATCH_ID       : Batch ID to process
 *   IN_INSURANCE_TYPE : Insurance type (LIF)
 *   IN_CONTRACT_TYPE  : Contract type (NEW / EXT)
 * Steps       :
 *   1. Hardcoded column mapping by contract type (NEW / EXT)
 *   2. Execute if column mapping is valid
 *      2.1. Create temp table
 *      2.2. Insert raw data into temp table
 *      2.3. Apply transformation rules (NEW / EXT)
 *      2.4. Insert transformed data into processed table
 *      2.5. Drop temp table
 */

CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_LNF`(
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
    DECLARE v_company_code VARCHAR(10)  DEFAULT 'LNF';
    DECLARE v_target_ym    VARCHAR(6)   DEFAULT '';
    DECLARE v_cutoff_ym    VARCHAR(6)   DEFAULT '';

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LNF_PROCESSED;
    END;

    -- [SET logic]
    SET v_target_ym = DATE_FORMAT(NOW(), '%Y%m');
    SET v_cutoff_ym = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 38 MONTH), '%Y%m');

    -- 1. Hardcoded Column Mapping for LINA Life (LNF)
    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        -- Mapping for NEW contracts (Columns 01-29 + Target-only 30, 31)
        SET v_raw_cols = ''; SET v_proc_cols = '';
        
        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_01, ', -- NO
            'COLUMN_02, ', -- 대리점
            'COLUMN_03, '); -- 대리점명
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_01, ', -- NO
            'COLUMN_02, ', -- 대리점
            'COLUMN_03, '); -- 대리점명
        
        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_04, ', -- 본부
            'COLUMN_05, ', -- 본부명
            'COLUMN_06, '); -- 지점
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_04, ', -- 본부
            'COLUMN_05, ', -- 본부명
            'COLUMN_06, '); -- 지점
        
        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_07, ', -- 지점명
            'COLUMN_08, ', -- 계약번호
            'COLUMN_09, '); -- 계약일자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_07, ', -- 지점명
            'COLUMN_08, ', -- 계약번호
            'COLUMN_09, '); -- 계약일자
        
        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_10, ', -- 계약상태
            'COLUMN_11, ', -- 보험료(KRW)
            'COLUMN_12, '); -- 보험료(USD)
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_10, ', -- 계약상태
            'COLUMN_11, ', -- 보험료(KRW)
            'COLUMN_12, '); -- 보험료(USD)
        
        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_13, ', -- 주계약보험료(KRW)
            'COLUMN_14, ', -- 특약보험료(KRW)
            'COLUMN_15, '); -- 출금일자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_13, ', -- 주계약보험료(KRW)
            'COLUMN_14, ', -- 특약보험료(KRW)
            'COLUMN_15, '); -- 출금일자
        
        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_16, ', -- 설계사
            'COLUMN_17, ', -- 설계사명
            'COLUMN_18, '); -- 계약자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_16, ', -- 설계사
            'COLUMN_17, ', -- 설계사명
            'COLUMN_18, '); -- 계약자
        
        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_19, ', -- 피보험자
            'COLUMN_20, ', -- 상품코드
            'COLUMN_21, '); -- 상품명
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_19, ', -- 피보험자
            'COLUMN_20, ', -- 상품코드
            'COLUMN_21, '); -- 상품명
        
        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_22, ', -- 가입금액
            'COLUMN_23, ', -- 월환산보험료
            'COLUMN_24, '); -- 연환산보험료
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_22, ', -- 가입금액
            'COLUMN_23, ', -- 월환산보험료
            'COLUMN_24, '); -- 연환산보험료
        
        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_25, ', -- CMP
            'COLUMN_26, ', -- 납입기간
            'COLUMN_27, '); -- 납입기간(년)
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_25, ', -- CMP
            'COLUMN_26, ', -- 납입기간
            'COLUMN_27, '); -- 납입기간(년)
        
        -- 28-29
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 납입주기
            'COLUMN_29, '); -- 납입방법
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 납입주기
            'COLUMN_29, '); -- 납입방법
        
        -- 30-31 (Target-only)
        SET v_raw_cols = CONCAT(v_raw_cols,
            'NULL, ', -- 납기구분
            'NULL');  -- 납입월
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_30, ', -- 납기구분
            'COLUMN_31');  -- 납입월

    ELSEIF UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN
        -- Mapping for EXT contracts (Columns 01-44)
        SET v_raw_cols = ''; SET v_proc_cols = '';
        
        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- NO
            'COLUMN_02, ', -- 영업본부
            'COLUMN_03, '); -- 영업본부명(GA기준)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- NO
            'COLUMN_02, ', -- 영업본부
            'COLUMN_03, '); -- 영업본부명(GA기준)
        
        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_04, ', -- 대리점
            'COLUMN_05, ', -- 대리점명
            'COLUMN_06, '); -- 본부
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_04, ', -- 대리점
            'COLUMN_05, ', -- 대리점명
            'COLUMN_06, '); -- 본부
        
        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_07, ', -- 본부명
            'COLUMN_08, ', -- 지점
            'COLUMN_09, '); -- 지점명
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_07, ', -- 본부명
            'COLUMN_08, ', -- 지점
            'COLUMN_09, '); -- 지점명
        
        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 수금설계사
            'COLUMN_11, ', -- 수금설계사명
            'COLUMN_12, '); -- 모집설계사
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 수금설계사
            'COLUMN_11, ', -- 수금설계사명
            'COLUMN_12, '); -- 모집설계사
        
        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 모집설계사명
            'COLUMN_14, ', -- 계약번호
            'COLUMN_15, '); -- 계약일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 모집설계사명
            'COLUMN_14, ', -- 계약번호
            'COLUMN_15, '); -- 계약일자
        
        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 계약자명
            'COLUMN_17, ', -- 주민등록번호
            'COLUMN_18, '); -- 계약자 주소 (시/도단위)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 계약자명
            'COLUMN_17, ', -- 주민등록번호
            'COLUMN_18, '); -- 계약자 주소 (시/도단위)
        
        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 피보험자명
            'COLUMN_20, ', -- 주민등록번호
            'COLUMN_21, '); -- 상품코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 피보험자명
            'COLUMN_20, ', -- 주민등록번호
            'COLUMN_21, '); -- 상품코드
        
        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 상품명
            'COLUMN_23, ', -- 통화구분
            'COLUMN_24, '); -- 가입금액
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 상품명
            'COLUMN_23, ', -- 통화구분
            'COLUMN_24, '); -- 가입금액
        
        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 합계보험료 (KRW)
            'COLUMN_26, ', -- 합계보험료 (USD)
            'COLUMN_27, '); -- 신계약CMP
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 합계보험료 (KRW)
            'COLUMN_26, ', -- 합계보험료 (USD)
            'COLUMN_27, '); -- 신계약CMP
        
        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 유지계약CMP
            'COLUMN_29, ', -- 계약상태
            'COLUMN_30, '); -- 계약상세상태
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 유지계약CMP
            'COLUMN_29, ', -- 계약상태
            'COLUMN_30, '); -- 계약상세상태
        
        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_31, ', -- 만기일자
            'COLUMN_32, ', -- 소멸일자
            'COLUMN_33, '); -- 납입방법
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_31, ', -- 만기일자
            'COLUMN_32, ', -- 소멸일자
            'COLUMN_33, '); -- 납입방법
        
        -- 34-36
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_34, ', -- 납입주기
            'COLUMN_35, ', -- 이체일자
            'COLUMN_36, '); -- 입금상태
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_34, ', -- 납입주기
            'COLUMN_35, ', -- 이체일자
            'COLUMN_36, '); -- 입금상태
        
        -- 37-39
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_37, ', -- 보험기간
            'COLUMN_38, ', -- 납입기간
            'COLUMN_39, '); -- 유지년월
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_37, ', -- 보험기간
            'COLUMN_38, ', -- 납입기간
            'COLUMN_39, '); -- 유지년월
        
        -- 40-42
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_40, ', -- 유지횟수
            'COLUMN_41, ', -- 최종납입일자
            'COLUMN_42, '); -- 최종납입방법
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_40, ', -- 유지횟수
            'COLUMN_41, ', -- 최종납입일자
            'COLUMN_42, '); -- 최종납입방법
        
        -- 43-44
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_43, ', -- 갱신여부
            'COLUMN_44'); -- 갱신계약번호
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_43, ', -- 갱신여부
            'COLUMN_44'); -- 갱신계약번호

    END IF;

    -- 2. Execute if column mapping is valid
    IF v_raw_cols != '' AND v_proc_cols != '' THEN
        
        -- 2.1. Create temp table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LNF_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_LNF_PROCESSED LIKE ', v_proc_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        -- 2.2. Insert raw data into temp table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_LNF_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''', v_company_code, ''', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM T_RPA_LIFE_RAW ',
            'WHERE COMPANY_CODE = ''', v_company_code, ''' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''');'
        );
        
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- 2.3. Apply transformation rules (NEW / EXT)
        IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            /* Rule 1: 맨 마지막열 값 추가(2개)
               ① 항목명I : 납기구분 / 항목값 : 년납
               ② 항목명II : 납입월 / 항목값 : 해당월도(ex.202512)
               ※ 전체 행에 반영
            */
            UPDATE T_TEMP_RPA_LNF_PROCESSED
            SET COLUMN_30 = '년납',
                COLUMN_31 = DATE_FORMAT(CURDATE(), '%Y%m');

        ELSEIF UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN
            /* Rule 1: [계약상태]="실효,시효,유지,청약"이면 [소멸일자]에 "0000-00-00"으로 수정 */
            UPDATE T_TEMP_RPA_LNF_PROCESSED
            SET COLUMN_32 = '0000-00-00'
            WHERE COLUMN_29 IN ('실효', '시효', '유지', '청약');

            /* Rule 2: [유지횟수]="0"이면, 제휴사 원부 조회하여 값 확인 후 수정(심사.계약>계약사항관리>"보험료입금조회" -> 납입회차(첫번째 행))
               -> 원부확인시 "영업제한 대상" 팝업뜨는 계약이면 데이터 행삭제
            */
            DELETE FROM T_TEMP_RPA_LNF_PROCESSED
            WHERE COLUMN_14 IN (
                SELECT SEARCH_DATA
                FROM T_RPA_INSURANCE_EXTRA_GUIDE
                WHERE SYS_FLAG = '1'
                  AND BATCH_ID = IN_BATCH_ID
                  AND COMPANY_CODE = v_company_code
                  AND CONTRACT_TYPE = 'EXT'
                  AND BUSINESS_RULE_NO = '2'
                  AND ACTION = 'DEL'
            );

            /* Rule 3: [계약상태]=“실효” & [유지년월]=“실효 3년 경과”면, [계약상태]값을 “시효”로 변경
               3년 경과 기준 : 마감월도 2025.12월 기준 최종납입월이 2022.10월 이하
            */
            UPDATE T_TEMP_RPA_LNF_PROCESSED
            SET COLUMN_29 = '시효'
            WHERE COLUMN_29 = '실효'
              AND COLUMN_39 IS NOT NULL
              AND COLUMN_39 <> ''
              AND REPLACE(COLUMN_39, '-', '') <= v_cutoff_ym;
        END IF;

        -- 2.4. Insert transformed data into processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_proc_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_LNF_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();

        -- 2.5. Drop temp table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LNF_PROCESSED;

    END IF;

END