CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_MRF`(
    IN IN_BATCH_ID       VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE  VARCHAR(20)
)
BEGIN
    -- [DECLARE variables]
    DECLARE v_row_count    INT          DEFAULT 0;
    DECLARE v_company_code VARCHAR(10)  DEFAULT 'MRF';
    DECLARE v_target_ym    VARCHAR(6)   DEFAULT '';
    DECLARE v_cutoff_ym    VARCHAR(6)   DEFAULT '';

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_MRF_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
    END;

    -- [SET internal logic]
    SET v_target_ym = DATE_FORMAT(NOW(), '%Y%m');
    -- Standard 38-month cutoff for Statute of Limitations (Rule 6 Existing)
    SET v_cutoff_ym = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 38 MONTH), '%Y%m');

    -- [Initialize Temporary Table with Explicit Structure]
    DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_MRF_PROCESSED;
    IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
        CREATE TEMPORARY TABLE T_TEMP_RPA_MRF_PROCESSED LIKE T_RPA_LONG_TERM_PROCESSED;
    ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
        CREATE TEMPORARY TABLE T_TEMP_RPA_MRF_PROCESSED LIKE T_RPA_CAR_PROCESSED;
    ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
        CREATE TEMPORARY TABLE T_TEMP_RPA_MRF_PROCESSED LIKE T_RPA_GENERAL_PROCESSED;
    END IF;

    -- ======================================================================
    -- A. CONTRACT TYPE : NEW (신계약) - 33 Columns Mapping
    -- ======================================================================
    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN

        -- [Mapping data from RAW to TEMP]
        INSERT INTO T_TEMP_RPA_MRF_PROCESSED (
            SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE,
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, -- 일자, 증권번호, 보험료, 납입주기, 횟수
            COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10, -- 수금방법, 영수, 선납, 초회수정P, 초년도수정P
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, -- 출생후보험료, 출생후수정P, 수정P, 계약자, 수수료분급
            COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20, -- 신규구분, 차량번호, 입력일, 리스크등급, 대리점설계사코드
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, -- 대리점설계사명, 지사명, 상품코드, 상품명, 고위험물건
            COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30, -- 공동물건, 행복나눔특약, 청약일, 납입기간, 납입월
            COLUMN_31, COLUMN_32, COLUMN_33                        -- 보험종료일자, 인수형태, 피보험자
        )
        SELECT
            REPLACE(UUID(), '-', ''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), v_company_code, BATCH_ID, CONTRACT_TYPE,
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05,
            COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15,
            COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25,
            COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30,
            COLUMN_31, COLUMN_32, COLUMN_33
        FROM T_RPA_MERITZ_RAW
        WHERE COMPANY_CODE = v_company_code AND BATCH_ID = IN_BATCH_ID AND UPPER(CONTRACT_TYPE) = 'NEW';

        -- [Rule 1] 맨 마지막열 값 추가: 납기구분='년납', 납입월=해당월
        UPDATE T_TEMP_RPA_MRF_PROCESSED SET COLUMN_34 = '년납', COLUMN_35 = v_target_ym;

        -- ------------------------------------------------------------------
        -- A.1. NEW LTR (장기 신계약) Logic
        -- ------------------------------------------------------------------
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
            -- [Rule 2] [영수] NOT IN '신계약','취소' -> 행 삭제
            DELETE FROM T_TEMP_RPA_MRF_PROCESSED WHERE COLUMN_07 NOT IN ('신계약', '취소');

            -- [Rule 3] [일자]!=해당월 & [상품명]!=실손 -> 행 삭제
            DELETE FROM T_TEMP_RPA_MRF_PROCESSED 
            WHERE LEFT(REPLACE(COLUMN_01, '-', ''), 6) <> v_target_ym AND COLUMN_24 NOT LIKE '%실손%';

            -- [Rule 4] 증권번호 중복 편집: '신계약'+'취소' 공존 시 '취소'로 통일 및 음수 삭제
            DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
            CREATE TEMPORARY TABLE tmp_dup_case SELECT COLUMN_02 FROM T_TEMP_RPA_MRF_PROCESSED GROUP BY COLUMN_02 HAVING SUM(COLUMN_07='신계약')>0 AND SUM(COLUMN_07='취소')>0;
            UPDATE T_TEMP_RPA_MRF_PROCESSED t INNER JOIN tmp_dup_case d ON t.COLUMN_02 = d.COLUMN_02 SET t.COLUMN_07 = '취소';
            DELETE FROM T_TEMP_RPA_MRF_PROCESSED WHERE COLUMN_02 IN (SELECT COLUMN_02 FROM tmp_dup_case) AND (COLUMN_03 LIKE '-%' OR CAST(REPLACE(COLUMN_03,',','') AS SIGNED) < 0);
        
        -- ------------------------------------------------------------------
        -- A.2. NEW CAR (자동차 신계약) Logic
        -- ------------------------------------------------------------------
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
            -- [Rule 2] 증권번호 중복 편집
            -- ② 모두 배서 삭제
            DELETE FROM T_TEMP_RPA_MRF_PROCESSED WHERE COLUMN_02 IN (SELECT * FROM (SELECT COLUMN_02 FROM T_TEMP_RPA_MRF_PROCESSED GROUP BY COLUMN_02 HAVING COUNT(*)>1 AND SUM(COLUMN_07<>'배서')=0) AS t);
            -- ③ 신계약+배서 중 배서 삭제
            DELETE t FROM T_TEMP_RPA_MRF_PROCESSED t WHERE COLUMN_07 = '배서' AND COLUMN_02 IN (SELECT * FROM (SELECT COLUMN_02 FROM T_TEMP_RPA_MRF_PROCESSED WHERE COLUMN_07='신계약') AS t);
            -- ④ 신계약+취소 -> [영수]값을 '취소'로 수정 및 음수 보험료 삭제
            DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
            CREATE TEMPORARY TABLE tmp_dup_case SELECT COLUMN_02 FROM T_TEMP_RPA_MRF_PROCESSED GROUP BY COLUMN_02 HAVING SUM(COLUMN_07='신계약')>0 AND SUM(COLUMN_07='취소')>0;
            UPDATE T_TEMP_RPA_MRF_PROCESSED t INNER JOIN tmp_dup_case d ON t.COLUMN_02 = d.COLUMN_02 SET t.COLUMN_07 = '취소';
            DELETE FROM T_TEMP_RPA_MRF_PROCESSED WHERE COLUMN_02 IN (SELECT COLUMN_02 FROM tmp_dup_case) AND (COLUMN_03 LIKE '-%' OR CAST(REPLACE(COLUMN_03,',','') AS SIGNED) < 0);

            -- [Rule 3] [보험료]="마이너스"이면 "플러스"값으로 수정
            UPDATE T_TEMP_RPA_MRF_PROCESSED SET COLUMN_03 = CAST(ABS(CAST(REPLACE(COLUMN_03, ',', '') AS SIGNED)) AS CHAR) WHERE COLUMN_03 LIKE '-%';

        -- ------------------------------------------------------------------
        -- A.3. NEW GEN (일반 신계약) Logic
        -- ------------------------------------------------------------------
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
            -- [Rule 2] 증권번호 중복 편집
            -- ② 모두 정상 삭제
            DELETE FROM T_TEMP_RPA_MRF_PROCESSED WHERE COLUMN_02 IN (SELECT * FROM (SELECT COLUMN_02 FROM T_TEMP_RPA_MRF_PROCESSED GROUP BY COLUMN_02 HAVING COUNT(*)>1 AND SUM(COLUMN_07<>'정상')=0) AS t);
            -- ③ 신계약+배서 중 배서 삭제
            DELETE t FROM T_TEMP_RPA_MRF_PROCESSED t WHERE COLUMN_07 = '배서' AND COLUMN_02 IN (SELECT * FROM (SELECT COLUMN_02 FROM T_TEMP_RPA_MRF_PROCESSED WHERE COLUMN_07='신계약') AS t);
            -- ④ 신계약+취소 -> [영수]값을 '취소'로 수정 및 음수 보험료 삭제
            DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
            CREATE TEMPORARY TABLE tmp_dup_case SELECT COLUMN_02 FROM T_TEMP_RPA_MRF_PROCESSED GROUP BY COLUMN_02 HAVING SUM(COLUMN_07='신계약')>0 AND SUM(COLUMN_07='취소')>0;
            UPDATE T_TEMP_RPA_MRF_PROCESSED t INNER JOIN tmp_dup_case d ON t.COLUMN_02 = d.COLUMN_02 SET t.COLUMN_07 = '취소';
            DELETE FROM T_TEMP_RPA_MRF_PROCESSED WHERE COLUMN_02 IN (SELECT COLUMN_02 FROM tmp_dup_case) AND (COLUMN_03 LIKE '-%' OR CAST(REPLACE(COLUMN_03,',','') AS SIGNED) < 0);
            -- ⑤ 청약일!=해당월 & [보험료]=음수 행 삭제
            DELETE FROM T_TEMP_RPA_MRF_PROCESSED WHERE LEFT(REPLACE(COLUMN_28, '-', ''), 6) <> v_target_ym AND (COLUMN_03 LIKE '-%' OR CAST(REPLACE(COLUMN_03,',','') AS SIGNED) < 0);
        END IF;

    -- ======================================================================
    -- B. CONTRACT TYPE : EXT (기존계약 - LTR) - 56 Columns Mapping
    -- ======================================================================
    ELSEIF UPPER(IN_CONTRACT_TYPE) IN ('EXT', 'EXISTING') THEN

        INSERT INTO T_TEMP_RPA_MRF_PROCESSED (
            SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE,
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, -- 업무시스템코드, 증권번호, 단위상품코드, 청약일자, 계약상태코드
            COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10, -- 보험개시일자, 보험종료일자, 최종납입년월, 최종납입회차, 영업보험료금액
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, -- 총납입보험료금액, 취급자코드, 취급자명, 대리점설계사코드, 대리점설계사명
            -- (Mappng 16-50 organizational data...)
            COLUMN_51, COLUMN_52, COLUMN_53, COLUMN_54, COLUMN_55, -- 피보험자명, 피보험자생년월일, 계약상태명, 소멸실효일자, 최종납입일자
            COLUMN_56                                              -- 계약상태상세명
        )
        SELECT
            REPLACE(UUID(), '-', ''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), v_company_code, BATCH_ID, CONTRACT_TYPE,
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15,
            COLUMN_51, COLUMN_52, COLUMN_53, COLUMN_54, COLUMN_55, COLUMN_56
        FROM T_RPA_MERITZ_RAW 
        WHERE COMPANY_CODE = v_company_code AND BATCH_ID = IN_BATCH_ID AND UPPER(CONTRACT_TYPE) = 'EXT';

        -- 1. [계약상태상세명] IN '정상, 해지, 해지불능' -> [소멸실효일자]='0000-00-00'
        UPDATE T_TEMP_RPA_MRF_PROCESSED SET COLUMN_54 = '0000-00-00' WHERE COLUMN_56 IN ('정상', '해지', '해지불능');

        -- 2. [최종납입년월] 연체건 처리 (M/D/YYYY -> YYYYMM comparison)
        UPDATE T_TEMP_RPA_MRF_PROCESSED SET COLUMN_53 = '연체'
        WHERE COLUMN_56 NOT LIKE '%완납%' AND COLUMN_56 NOT LIKE '%납입면제%' AND COLUMN_53 = '정상'
          AND DATE_FORMAT(STR_TO_DATE(COLUMN_08, '%c/%e/%Y'), '%Y%m') < v_target_ym;

        -- 3. [계약상세상태명]='중지' -> [계약상태명]='정상'
        UPDATE T_TEMP_RPA_MRF_PROCESSED SET COLUMN_53 = '정상' WHERE COLUMN_56 = '중지';

        -- 4. Ngày tháng chuẩn hóa (M/D/YYYY -> YYYY-MM-DD)
        UPDATE T_TEMP_RPA_MRF_PROCESSED SET 
            COLUMN_04 = DATE_FORMAT(STR_TO_DATE(COLUMN_04, '%c/%e/%Y'), '%Y-%m-%d'),
            COLUMN_06 = DATE_FORMAT(STR_TO_DATE(COLUMN_06, '%c/%e/%Y'), '%Y-%m-%d'),
            COLUMN_07 = DATE_FORMAT(STR_TO_DATE(COLUMN_07, '%c/%e/%Y'), '%Y-%m-%d');

        -- 5. [계약상세상태명] IN '취소, 철회' -> [최종납입일자]=[청약일자]
        UPDATE T_TEMP_RPA_MRF_PROCESSED SET COLUMN_55 = COLUMN_04 WHERE COLUMN_56 IN ('취소', '철회');

        -- 6. [시효] 처리 (실효 & 38개월 경과)
        UPDATE T_TEMP_RPA_MRF_PROCESSED SET COLUMN_53 = '시효'
        WHERE COLUMN_56 = '실효' AND DATE_FORMAT(STR_TO_DATE(COLUMN_08, '%c/%e/%Y'), '%Y%m') <= v_cutoff_ym;

    END IF;

    -- [FINAL EXPLICIT INSERT TO PROCESS TABLES - No SELECT *]
    IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
        INSERT INTO T_RPA_LONG_TERM_PROCESSED (
            SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE,
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30,
            COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34, COLUMN_35, COLUMN_36, COLUMN_37, COLUMN_38, COLUMN_39, COLUMN_40,
            COLUMN_41, COLUMN_42, COLUMN_43, COLUMN_44, COLUMN_45, COLUMN_46, COLUMN_47, COLUMN_48, COLUMN_49, COLUMN_50,
            COLUMN_51, COLUMN_52, COLUMN_53, COLUMN_54, COLUMN_55, COLUMN_56
        )
        SELECT 
            SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE,
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30,
            COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34, COLUMN_35, COLUMN_36, COLUMN_37, COLUMN_38, COLUMN_39, COLUMN_40,
            COLUMN_41, COLUMN_42, COLUMN_43, COLUMN_44, COLUMN_45, COLUMN_46, COLUMN_47, COLUMN_48, COLUMN_49, COLUMN_50,
            COLUMN_51, COLUMN_52, COLUMN_53, COLUMN_54, COLUMN_55, COLUMN_56
        FROM T_TEMP_RPA_MRF_PROCESSED;
    
    ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
        INSERT INTO T_RPA_CAR_PROCESSED (
            SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE,
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30,
            COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34
        )
        SELECT 
            SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE,
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30,
            COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34
        FROM T_TEMP_RPA_MRF_PROCESSED;

    ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
        INSERT INTO T_RPA_GENERAL_PROCESSED (
            SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE,
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30,
            COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34
        )
        SELECT 
            SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE,
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30,
            COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34
        FROM T_TEMP_RPA_MRF_PROCESSED;
    END IF;

    -- [Cleanup Operations]
    SET v_row_count = ROW_COUNT();
    DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_MRF_PROCESSED;
    DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;

END