CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_KBL`(
    IN IN_BATCH_ID       VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE  VARCHAR(20)
)
BEGIN
    -- DECLARE variables
    DECLARE v_row_count  INT        DEFAULT 0;
    DECLARE v_company_code VARCHAR(10) DEFAULT 'KBL';
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

    -- ======================================================================
    --  CONTRACT TYPE : NEW (신계약) - 34 Columns Mapping
    -- ======================================================================
    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        
        INSERT INTO T_TEMP_RPA_LIFE_PROCESSED (
            SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX,
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30,
            COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34
        )
        SELECT
            REPLACE(UUID(), '-', ''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), v_company_code, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX,
            -- 지점명 | 수금설계 | 수금코드 | 증권번호 | 계약자
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, 
            -- 피보험 | 상품 | 상품코드 | 계약일자 | 상태
            COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10, 
            -- 계약상태 | 보험료 | 실납입P | 달러P | 선수선납P
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, 
            -- 최종납입월 | 최종월(예) | 최종횟수 | 최종회(예) | 최종납일
            COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20, 
            -- 차기납일 | 수금방법 | 납입방법 | 이체일 | 납입기간
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, 
            -- 상태변경일 | 만기소멸일 | 확정회차 | 모집설계 | 모집코드
            COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30,
            -- 가입금액 | 초년도 | 2차년도 | 3차년도
            COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34
        FROM T_RPA_LIFE_RAW
        WHERE COMPANY_CODE = v_company_code AND BATCH_ID = IN_BATCH_ID AND UPPER(CONTRACT_TYPE) = 'NEW';

        -- [Rule 2] 항목명 : 납기구분 / 항목값 : 년납
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_35 = '년납' WHERE UPPER(CONTRACT_TYPE) = 'NEW';

    -- ======================================================================
    --  CONTRACT TYPE : EXT (Existing Contract) - 34 Columns Mapping
    -- ======================================================================
    ELSEIF UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN
        
        INSERT INTO T_TEMP_RPA_LIFE_PROCESSED (
            SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX,
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30,
            COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34
        )
        SELECT
            REPLACE(UUID(), '-', ''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), v_company_code, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX,
            -- 지점명 | 수금설계 | 수금코드 | 증권번호 | 계약자
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, 
            -- 피보험 | 상품 | 상품코드 | 계약일자 | 상태
            COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10, 
            -- 계약상태 | 보험료 | 실납입P | 달러P | 선수선납P
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, 
            -- 최종납입월 | 최종월(예) | 최종횟수 | 최종회(예) | 최종납일
            COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20, 
            -- 차기납일 | 수금방법 | 납입방법 | 이체일 | 납입기간
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, 
            -- 상태변경일 | 만기소멸일 | 확정회차 | 모집설계 | 모집코드
            COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30,
            -- 가입금액 | 초년도 | 2차년도 | 3차년도
            COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34
        FROM T_RPA_LIFE_RAW
        WHERE COMPANY_CODE = v_company_code AND BATCH_ID = IN_BATCH_ID AND UPPER(CONTRACT_TYPE) = UPPER(IN_CONTRACT_TYPE);

        -- [Rule 1] 계약상태변경일 편집
        -- (1) [상태]="계류,정상"이면 [계약상태변경일](26)을 "0000-00-00"으로 수정
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_26 = '0000-00-00'
        WHERE COLUMN_10 IN ('계류', '정상');

        -- (2) [상태]=“종료” & [계약상태] IN (“실효(환급금없는실효)”, “실효(환급금있는실효)”) -> [계약상태변경일]=“0000-00-00”
        UPDATE T_TEMP_RPA_LIFE_PROCESSED
        SET COLUMN_26 = '0000-00-00'
        WHERE COLUMN_10 = '종료'
        AND COLUMN_11 IN ('실효(환급금없는실효)', '실효(환급금있는실효)');

        -- (2) [상태]="종료" & [계약상태]="실효(환급금없는실효) hoặc 실효(환급금있는실효)"면 [계약상태변경일]=0000-00-00
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_26 = '0000-00-00'
        WHERE COLUMN_10 = '종료' AND COLUMN_11 IN ('실효(환급금없는실효)', '실효(환급금있는실효)');

        -- [Rule 2.1] [최종납입월](16) 빈셀 or 1900-01 & [계약상태]="계류(성립이전),반송,철회"면 [최종납입월]=계약년월
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_16 = LEFT(COLUMN_09, 7)
        WHERE (COLUMN_16 IS NULL OR COLUMN_16 = '' OR COLUMN_16 = '1900-01')
          AND COLUMN_11 IN ('계류(성립이전)', '반송', '철회');

        -- [Rule 2.2] [최종횟수] 빈셀 or 0 & [계약상태]="계류(성립이전),반송,철회"면 [최종횟수]="1" (PAUSED/SKIP)
        /*
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_18 = '1'
        WHERE (COLUMN_18 IS NULL OR COLUMN_18 = '' OR COLUMN_18 = '0')
          AND COLUMN_11 IN ('계류(성립이전)', '반송', '철회');
        */

        -- [Rule 2.3] [납입방법](23) 빈셀이면 -> [최종횟수](18)="1"이면 [최종납입일](20)=계약일자, 아니면 0000-00-00
        UPDATE T_TEMP_RPA_LIFE_PROCESSED 
        SET COLUMN_20 = CASE WHEN COLUMN_18 = '1' THEN COLUMN_09 ELSE '0000-00-00' END
        WHERE COLUMN_23 IS NULL OR COLUMN_23 = '';

        -- [Rule 3] [납입방법](23)="일시납"이면 (1) [보험료](12)="0" (2) [최종횟수](18)="1"
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_12 = '0', COLUMN_18 = '1' 
        WHERE COLUMN_23 = '일시납';

        -- [Rule 4] [계약상세상태]="신계약" & [최종납입월]=2개월전 -> [계약상태]="실효" (PAUSED/SKIP)
        /*
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_11 = '실효'
        WHERE COLUMN_11 = '신계약'
          AND REPLACE(COLUMN_16, '-', '') = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 2 MONTH), '%Y%m');
        */

        -- [Rule 5] [계약상태]="실효" & 실효 3년 경과(38개월) -> [계약상태]="시효" (PAUSED/SKIP)
        /*
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_11 = '시효'
        WHERE COLUMN_11 = '실효'
          AND REPLACE(LEFT(COLUMN_16, 7), '-', '') <= v_cutoff_ym;
        */

    END IF;

    -- ======================================================================
    --  FINAL INSERT TO PROCESSED
    -- ======================================================================
    INSERT INTO T_RPA_LIFE_PROCESSED (
        SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX,
        COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
        COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
        COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30,
        COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34, COLUMN_35
    )
    SELECT
        SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX,
        COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
        COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
        COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30,
        COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34, COLUMN_35
    FROM T_TEMP_RPA_LIFE_PROCESSED;

    SET v_row_count = ROW_COUNT();
    DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LIFE_PROCESSED;

END