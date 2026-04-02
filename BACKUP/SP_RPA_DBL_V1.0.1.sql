CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_DBL`(
    IN IN_BATCH_ID       VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE  VARCHAR(20)
)
BEGIN
    -- DECLARE variables
    DECLARE v_row_count  INT        DEFAULT 0;
    DECLARE v_company_code VARCHAR(10) DEFAULT 'DBL';
    DECLARE v_target_ym  VARCHAR(6) DEFAULT '';
    DECLARE v_cutoff_ym  VARCHAR(6) DEFAULT '';

    -- DECLARE handler
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LIFE_PROCESSED;
    END;

    -- SET / logic
    SET v_target_ym = DATE_FORMAT(NOW(), '%Y%m');
    -- Rule 5 cutoff: 38 months
    SET v_cutoff_ym = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 38 MONTH), '%Y%m');

    -- 1. Create temporary table
    DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LIFE_PROCESSED;
    CREATE TEMPORARY TABLE T_TEMP_RPA_LIFE_PROCESSED LIKE T_RPA_LIFE_PROCESSED;

    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        SELECT 1;
    END IF;

    -- ======================================================================
    --  CONTRACT TYPE : EXT (보유계약) - 30 Columns Mapping
    -- ======================================================================
    IF UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN
        
        INSERT INTO T_TEMP_RPA_LIFE_PROCESSED (
            SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX,
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30
        )
        SELECT
            REPLACE(UUID(), '-', ''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), v_company_code, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX,
            -- 증권번호 | 계약자 | 보험료 | 납입횟수 | 계약년월
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, 
            -- 계약일자 | 종납년월 | UV종납년월 | UV종납횟수 | 모집코드
            COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10, 
            -- 모집사원명 | 환산보험료 | 1차환산P | 2차환산P | 3차환산P
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, 
            -- 4차환산P | 보험종류 | 상태 | 소멸일자 | 납입주기
            COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20, 
            -- 납입기간 | 수금방법 | 주계약보종명 | 수금사원코드 | 수금사원명
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, 
            -- 최종납입일자 | 지점 | 지사 | 생성일시 | 조회구분
            COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30
        FROM T_RPA_LIFE_RAW
        WHERE COMPANY_CODE = v_company_code AND BATCH_ID = IN_BATCH_ID AND UPPER(CONTRACT_TYPE) = UPPER(IN_CONTRACT_TYPE);

        -- [Rule 2] 상태=실효,연체,완납,정상 & UV종납년월=값있음 (PAUSED/SKIPPED)
        /*
        UPDATE T_TEMP_RPA_LIFE_PROCESSED
        SET COLUMN_26 = COLUMN_08, COLUMN_04 = COLUMN_09
        WHERE COLUMN_18 IN ('실효', '연체', '완납', '정상') AND COLUMN_08 IS NOT NULL AND COLUMN_08 <> '';
        */

        -- [Rule 3] [상태]=실효,연체,완납,정상이면 [소멸일자]값을 “0000-00-00”으로 수정
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_19 = '0000-00-00'
        WHERE UPPER(CONTRACT_TYPE) = UPPER(IN_CONTRACT_TYPE) AND COLUMN_18 IN ('실효', '연체', '완납', '정상');

        -- [Rule 4] [납입주기]=“일시납”이면 [보험료]=“0”으로 수정, [납입횟수]=“1”로 수정
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_03 = '0', COLUMN_04 = '1'
        WHERE UPPER(CONTRACT_TYPE) = UPPER(IN_CONTRACT_TYPE) AND COLUMN_20 = '일시납';

        -- [Rule 5] [상태]=실효 & [최종납입일자]=실효 3년 경과면, [상태]값을 “시효"로 변경
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_18 = '시효'
        WHERE UPPER(CONTRACT_TYPE) = UPPER(IN_CONTRACT_TYPE) AND COLUMN_18 = '실효'
          AND COLUMN_26 IS NOT NULL AND COLUMN_26 <> ''
          AND LEFT(REPLACE(REPLACE(COLUMN_26, '-', ''), '.', ''), 6) <= v_cutoff_ym;
    END IF;

    -- ======================================================================
    --  FINAL INSERT TO PROCESSED
    -- ======================================================================
    INSERT INTO T_RPA_LIFE_PROCESSED (
        SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX,
        COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
        COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
        COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30
    )
    SELECT
        SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX,
        COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
        COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
        COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30
    FROM T_TEMP_RPA_LIFE_PROCESSED;

    SET v_row_count = ROW_COUNT();
    DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LIFE_PROCESSED;
END