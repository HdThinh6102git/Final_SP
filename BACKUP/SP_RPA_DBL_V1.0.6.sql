/*
 * SP_RPA_DBL
 * Description : Process DB Life insurance data
 * Parameters  :
 *   IN IN_BATCH_ID       : Batch ID to process
 *   IN IN_INSURANCE_TYPE : Insurance type (LIF)
 *   IN IN_CONTRACT_TYPE  : Contract type (EXT only)
 * Steps       :
 *   1. Hardcoded column mapping by contract type (EXT)
 *   2. Execute if column mapping is valid
 *      2.1. Create temp table
 *      2.2. Insert raw data into temp table
 *      2.3. Apply transformation rules (EXT)
 *      2.4. Insert transformed data into processed table
 *      2.5. Drop temp table
 */

CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_DBL`(
    IN IN_BATCH_ID       VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE  VARCHAR(20)
)
BEGIN
    -- [DECLARE variables]
    DECLARE v_raw_cols        TEXT         DEFAULT '';
    DECLARE v_proc_cols       TEXT         DEFAULT '';
    DECLARE v_row_count       INT          DEFAULT 0;
    DECLARE v_company_code    VARCHAR(10)  DEFAULT 'DBL';
    DECLARE v_raw_table       VARCHAR(100) DEFAULT 'T_RPA_LIFE_RAW';
    DECLARE v_processed_table VARCHAR(100) DEFAULT 'T_RPA_LIFE_PROCESSED';

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_DBL_PROCESSED;
    END;

    -- 1. Hardcoded Column Mapping
    IF UPPER(IN_INSURANCE_TYPE) = 'LIF' AND UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN
        -- Mapping for EXT contracts (Columns 01-30)
        SET v_raw_cols = ''; SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 증권번호
            'COLUMN_02, ', -- 계약자
            'COLUMN_03, '); -- 보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 증권번호
            'COLUMN_02, ', -- 계약자
            'COLUMN_03, '); -- 보험료

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 납입횟수
            'COLUMN_05, ', -- 계약년월
            'COLUMN_06, '); -- 계약일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 납입횟수
            'COLUMN_05, ', -- 계약년월
            'COLUMN_06, '); -- 계약일자

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 종납년월
            'COLUMN_08, ', -- UV종납년월
            'COLUMN_09, '); -- UV종납횟수
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 종납년월
            'COLUMN_08, ', -- UV종납년월
            'COLUMN_09, '); -- UV종납횟수

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 모집코드
            'COLUMN_11, ', -- 모집사원명
            'COLUMN_12, '); -- 환산보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 모집코드
            'COLUMN_11, ', -- 모집사원명
            'COLUMN_12, '); -- 환산보험료

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 1차환산P
            'COLUMN_14, ', -- 2차환산P
            'COLUMN_15, '); -- 3차환산P
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 1차환산P
            'COLUMN_14, ', -- 2차환산P
            'COLUMN_15, '); -- 3차환산P

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 4차환산P
            'COLUMN_17, ', -- 보험종류
            'COLUMN_18, '); -- 상태
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 4차환산P
            'COLUMN_17, ', -- 보험종류
            'COLUMN_18, '); -- 상태

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 소멸일자
            'COLUMN_20, ', -- 납입주기
            'COLUMN_21, '); -- 납입기간
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 소멸일자
            'COLUMN_20, ', -- 납입주기
            'COLUMN_21, '); -- 납입기간

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 수금방법
            'COLUMN_23, ', -- 주계약보종명
            'COLUMN_24, '); -- 수금사원코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 수금방법
            'COLUMN_23, ', -- 주계약보종명
            'COLUMN_24, '); -- 수금사원코드

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 수금사원명
            'COLUMN_26, ', -- 최종납입일자
            'COLUMN_27, '); -- 지점
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 수금사원명
            'COLUMN_26, ', -- 최종납입일자
            'COLUMN_27, '); -- 지점

        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 지사
            'COLUMN_29, ', -- 생성일시
            'COLUMN_30');  -- 조회구분
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 지사
            'COLUMN_29, ', -- 생성일시
            'COLUMN_30');  -- 조회구분
    END IF;

    -- 2. Execute if column mapping is valid
    IF v_raw_cols != '' AND v_proc_cols != '' THEN

        -- 2.1. Create temp table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_DBL_PROCESSED;
        CREATE TEMPORARY TABLE T_TEMP_RPA_DBL_PROCESSED LIKE T_RPA_LIFE_PROCESSED;

        -- 2.2. Insert raw data into temp table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_DBL_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''', v_company_code, ''', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''', v_company_code, ''' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND CONTRACT_TYPE = UPPER(''', IN_CONTRACT_TYPE, ''');'
        );

        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- 2.3. Apply transformation rules (EXT)
        IF UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN

            /* Rule 2: [상태]=실효,연체,완납,정상 & [UV종납년월]=값있음 이면
               ① [종납년월]값을 [UV종납년월]로 수정
               ② [납입횟수]값을 [UV종납횟수]로 수정
            */
            UPDATE T_TEMP_RPA_DBL_PROCESSED
            SET COLUMN_07 = COLUMN_08,
                COLUMN_04 = COLUMN_09
            WHERE COLUMN_18 IN ('실효', '연체', '완납', '정상')
              AND COLUMN_08 IS NOT NULL
              AND COLUMN_08 <> '';

            /* Rule 3: [상태]=실효,연체,완납,정상이면
               [소멸일자]값을 "0000-00-00"으로 수정
            */
            UPDATE T_TEMP_RPA_DBL_PROCESSED
            SET COLUMN_19 = '0000-00-00'
            WHERE COLUMN_18 IN ('실효', '연체', '완납', '정상');

            /* Rule 4: [납입주기]="일시납"이면
               [보험료]="0"으로 수정
               [납입횟수]="1"로 수정
            */
            UPDATE T_TEMP_RPA_DBL_PROCESSED
            SET COLUMN_03 = '0',
                COLUMN_04 = '1'
            WHERE COLUMN_20 = '일시납';

            /* Rule 5: [상태]=실효 & [최종납입월]=실효 3년 경과면, [상태]값을 "시효"로 변경
               3년 경과 기준 : 현재월 기준 38개월 경과
            */
            UPDATE T_TEMP_RPA_DBL_PROCESSED
            SET COLUMN_18 = '시효'
            WHERE COLUMN_18 = '실효'
              AND COLUMN_26 IS NOT NULL
              AND COLUMN_26 <> ''
              AND PERIOD_DIFF(
                    DATE_FORMAT(CURDATE(), '%Y%m'),
                    LEFT(REPLACE(REPLACE(COLUMN_26, '-', ''), '.', ''), 6)
                  ) >= 38;
        END IF;

        -- 2.4. Insert transformed data into processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_processed_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_DBL_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();

        -- 2.5. Drop temp table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_DBL_PROCESSED;

    END IF;

END