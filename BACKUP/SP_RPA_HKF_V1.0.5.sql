CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_HKF`(
    IN IN_BATCH_ID       VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE  VARCHAR(20)
)
BEGIN
    -- [DECLARE variables]
    DECLARE v_raw_cols     TEXT         DEFAULT '';
    DECLARE v_proc_cols    TEXT         DEFAULT '';
    DECLARE v_sql_query    TEXT         DEFAULT '';
    DECLARE v_raw_table    VARCHAR(100) DEFAULT '';
    DECLARE v_proc_table   VARCHAR(100) DEFAULT '';
    DECLARE v_row_count    INT          DEFAULT 0;
    DECLARE v_company_code VARCHAR(10)  DEFAULT 'HKF';
    DECLARE v_target_ym    VARCHAR(6)   DEFAULT '';

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_HKF_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_sorted_hkf;
        DROP TEMPORARY TABLE IF EXISTS tmp_dup_chulhoe_hkf;
        DROP TEMPORARY TABLE IF EXISTS tmp_dup_gen_hkf;
    END;

--     SET v_target_ym = DATE_FORMAT(NOW(), '%Y%m');
   SET v_target_ym = '202603';
    -- I. Mapping Columns
    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        -- Mapping for LTR (Columns 01-30 + New Columns 31-32)
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';
            
            -- 01-03
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_01, ', -- 순번
                'COLUMN_02, ', -- 영수일
                'COLUMN_03, '); -- 계약일자
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_01, ', -- 순번
                'COLUMN_02, ', -- 영수일
                'COLUMN_03, '); -- 계약일자
            
            -- 04-06
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_04, ', -- 계약번호
                'COLUMN_05, ', -- 상품코드
                'COLUMN_06, '); -- 상품명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_04, ', -- 계약번호
                'COLUMN_05, ', -- 상품코드
                'COLUMN_06, '); -- 상품명
            
            -- 07-09
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_07, ', -- 계약회차
                'COLUMN_08, ', -- 수수료회차
                'COLUMN_09, '); -- 계약자명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_07, ', -- 계약회차
                'COLUMN_08, ', -- 수수료회차
                'COLUMN_09, '); -- 계약자명
            
            -- 10-12
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_10, ', -- 피보험자명
                'COLUMN_11, ', -- 상태
                'COLUMN_12, '); -- 납방
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_10, ', -- 피보험자명
                'COLUMN_11, ', -- 상태
                'COLUMN_12, '); -- 납방
            
            -- 13-15
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_13, ', -- 신계약 CSM
                'COLUMN_14, ', -- 영수보험료
                'COLUMN_15, '); -- 합계보험료
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_13, ', -- 신계약 CSM
                'COLUMN_14, ', -- 영수보험료
                'COLUMN_15, '); -- 합계보험료
            
            -- 16-18
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_16, ', -- 보장영수P
                'COLUMN_17, ', -- 적립영수P
                'COLUMN_18, '); -- 기타영수P
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_16, ', -- 보장영수P
                'COLUMN_17, ', -- 적립영수P
                'COLUMN_18, '); -- 기타영수P
            
            -- 19-21
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_19, ', -- 월납환산
                'COLUMN_20, ', -- 실손수정P
                'COLUMN_21, '); -- 실손외수정P
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_19, ', -- 월납환산
                'COLUMN_20, ', -- 실손수정P
                'COLUMN_21, '); -- 실손외수정P
            
            -- 22-24
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_22, ', -- 수정보험료
                'COLUMN_23, ', -- 납기
                'COLUMN_24, '); -- 만기
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_22, ', -- 수정보험료
                'COLUMN_23, ', -- 납기
                'COLUMN_24, '); -- 만기
            
            -- 25-27
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_25, ', -- 보장/적립
                'COLUMN_26, ', -- 태아여부
                'COLUMN_27, '); -- 사용인
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_25, ', -- 보장/적립
                'COLUMN_26, ', -- 태아여부
                'COLUMN_27, '); -- 사용인
            
            -- 28-30
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_28, ', -- 사용인명
                'COLUMN_29, ', -- 취급자
                'COLUMN_30, '); -- 취급자명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_28, ', -- 사용인명
                'COLUMN_29, ', -- 취급자
                'COLUMN_30, '); -- 취급자명

            -- 31-32
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'NULL, ', -- 납기구분
                'NULL'); -- 납입월
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_31, ', -- 납기구분
                'COLUMN_32'); -- 납입월

        -- Mapping for CAR (Columns 01-30 + New Columns 31-35)
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';

            -- 01-03
            SET v_raw_cols = CONCAT(v_raw_cols,
                'COLUMN_01, ', -- 순번
                'COLUMN_02, ', -- 영수일
                'COLUMN_03, '); -- 계약일자
            SET v_proc_cols = CONCAT(v_proc_cols,
                'COLUMN_01, ', -- 순번
                'COLUMN_02, ', -- 영수일
                'COLUMN_03, '); -- 계약일자

            -- 04-06
            SET v_raw_cols = CONCAT(v_raw_cols,
                'COLUMN_04, ', -- 계약번호
                'COLUMN_05, ', -- 상품코드
                'COLUMN_06, '); -- 상품명
            SET v_proc_cols = CONCAT(v_proc_cols,
                'COLUMN_04, ', -- 계약번호
                'COLUMN_05, ', -- 상품코드
                'COLUMN_06, '); -- 상품명

            -- 07-09
            SET v_raw_cols = CONCAT(v_raw_cols,
                'COLUMN_07, ', -- 계약회차
                'COLUMN_08, ', -- 수수료회차
                'COLUMN_09, '); -- 계약자명
            SET v_proc_cols = CONCAT(v_proc_cols,
                'COLUMN_07, ', -- 계약회차
                'COLUMN_08, ', -- 수수료회차
                'COLUMN_09, '); -- 계약자명

            -- 10-12
            SET v_raw_cols = CONCAT(v_raw_cols,
                'COLUMN_10, ', -- 피보험자명
                'COLUMN_11, ', -- 상태
                'COLUMN_12, '); -- 납방
            SET v_proc_cols = CONCAT(v_proc_cols,
                'COLUMN_10, ', -- 피보험자명
                'COLUMN_11, ', -- 상태
                'COLUMN_12, '); -- 납방

            -- 13-15
            SET v_raw_cols = CONCAT(v_raw_cols,
                'COLUMN_13, ', -- 신계약 CSM
                'COLUMN_14, ', -- 영수보험료
                'COLUMN_15, '); -- 합계보험료
            SET v_proc_cols = CONCAT(v_proc_cols,
                'COLUMN_13, ', -- 신계약 CSM
                'COLUMN_14, ', -- 영수보험료
                'COLUMN_15, '); -- 합계보험료

            -- 16-18
            SET v_raw_cols = CONCAT(v_raw_cols,
                'COLUMN_16, ', -- 보장영수P
                'COLUMN_17, ', -- 적립영수P
                'COLUMN_18, '); -- 기타영수P
            SET v_proc_cols = CONCAT(v_proc_cols,
                'COLUMN_16, ', -- 보장영수P
                'COLUMN_17, ', -- 적립영수P
                'COLUMN_18, '); -- 기타영수P

            -- 19-21
            SET v_raw_cols = CONCAT(v_raw_cols,
                'COLUMN_19, ', -- 월납환산
                'COLUMN_20, ', -- 실손수정P
                'COLUMN_21, '); -- 실손외수정P
            SET v_proc_cols = CONCAT(v_proc_cols,
                'COLUMN_19, ', -- 월납환산
                'COLUMN_20, ', -- 실손수정P
                'COLUMN_21, '); -- 실손외수정P

            -- 22-24
            SET v_raw_cols = CONCAT(v_raw_cols,
                'COLUMN_22, ', -- 수정보험료
                'COLUMN_23, ', -- 납기
                'COLUMN_24, '); -- 만기
            SET v_proc_cols = CONCAT(v_proc_cols,
                'COLUMN_22, ', -- 수정보험료
                'COLUMN_23, ', -- 납기
                'COLUMN_24, '); -- 만기

            -- 25-27
            SET v_raw_cols = CONCAT(v_raw_cols,
                'COLUMN_25, ', -- 보장/적립
                'COLUMN_26, ', -- 태아여부
                'COLUMN_27, '); -- 사용인
            SET v_proc_cols = CONCAT(v_proc_cols,
                'COLUMN_25, ', -- 보장/적립
                'COLUMN_26, ', -- 태아여부
                'COLUMN_27, '); -- 사용인

            -- 28-30
            SET v_raw_cols = CONCAT(v_raw_cols,
                'COLUMN_28, ', -- 사용인명
                'COLUMN_29, ', -- 취급자
                'COLUMN_30, '); -- 취급자명
            SET v_proc_cols = CONCAT(v_proc_cols,
                'COLUMN_28, ', -- 사용인명
                'COLUMN_29, ', -- 취급자
                'COLUMN_30, '); -- 취급자명

            -- 31-33 (Add New Columns)
            SET v_raw_cols = CONCAT(v_raw_cols,
                'NULL, ', -- 납기구분
                'NULL, ', -- 납입월
                'NULL, '); -- 납입일
            SET v_proc_cols = CONCAT(v_proc_cols,
                'COLUMN_31, ', -- 납기구분
                'COLUMN_32, ', -- 납입월
                'COLUMN_33, '); -- 납입일

            -- 34-35
            SET v_raw_cols = CONCAT(v_raw_cols,
                'NULL, ', -- 만기일자
                'NULL');  -- 차량번호
            SET v_proc_cols = CONCAT(v_proc_cols,
                'COLUMN_34, ', -- 만기일자
                'COLUMN_35');  -- 차량번호

        -- Mapping for GEN (Columns 01-20 + New Columns 21-26)
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';
            
            -- 01-03
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_01, ', -- 순번
                'COLUMN_02, ', -- 영수일
                'COLUMN_03, '); -- 계약일자
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_01, ', -- 순번
                'COLUMN_02, ', -- 영수일
                'COLUMN_03, '); -- 계약일자
            
            -- 04-06
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_04, ', -- 계약만료일자
                'COLUMN_05, ', -- 계약번호
                'COLUMN_06, '); -- 상품코드
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_04, ', -- 계약만료일자
                'COLUMN_05, ', -- 계약번호
                'COLUMN_06, '); -- 상품코드
            
            -- 07-09
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_07, ', -- 상품명
                'COLUMN_08, ', -- 회차
                'COLUMN_09, '); -- 계약자명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_07, ', -- 상품명
                'COLUMN_08, ', -- 회차
                'COLUMN_09, '); -- 계약자명
            
            -- 10-12
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_10, ', -- 피보험자명
                'COLUMN_11, ', -- 상태
                'COLUMN_12, '); -- 납방
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_10, ', -- 피보험자명
                'COLUMN_11, ', -- 상태
                'COLUMN_12, '); -- 납방
            
            -- 13-15
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_13, ', -- 인수구분
                'COLUMN_14, ', -- 대상
                'COLUMN_15, '); -- 영수보험료
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_13, ', -- 인수구분
                'COLUMN_14, ', -- 대상
                'COLUMN_15, '); -- 영수보험료
            
            -- 16-18
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_16, ', -- 월납환산
                'COLUMN_17, ', -- 사용인
                'COLUMN_18, '); -- 사용인명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_16, ', -- 월납환산
                'COLUMN_17, ', -- 사용인
                'COLUMN_18, '); -- 사용인명
            
            -- 19-21
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_19, ', -- 취급자
                'COLUMN_20, ', -- 취급자명
                'NULL, '); -- 납기구분
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_19, ', -- 취급자
                'COLUMN_20, ', -- 취급자명
                'COLUMN_21, '); -- 납기구분
            
            -- 22-24
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'NULL, ', -- 납입월
                'NULL, ', -- 납입주기
                'NULL, '); -- 납기
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_22, ', -- 납입월
                'COLUMN_23, ', -- 납입주기
                'COLUMN_24, '); -- 납기
            
            -- 25-26
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'NULL, ', -- 만기일자
                'NULL'); -- 보험사성적
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_25, ', -- 만기일자
                'COLUMN_26'); -- 보험사성적
        END IF;
    END IF;

    -- II. Handling Logics
    IF v_raw_cols != '' AND v_proc_cols != '' THEN
        
        -- 2.1. Select Tables based on Insurance Type
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
            SET v_raw_table = 'T_RPA_LONG_TERM_RAW';
            SET v_proc_table = 'T_RPA_LONG_TERM_PROCESSED';
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
            SET v_raw_table = 'T_RPA_CAR_RAW';
            SET v_proc_table = 'T_RPA_CAR_PROCESSED';    
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
            SET v_raw_table = 'T_RPA_GENERAL_RAW';
            SET v_proc_table = 'T_RPA_GENERAL_PROCESSED';
        END IF;

        -- 2.2. Create Temporary Table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_HKF_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_HKF_PROCESSED LIKE ', v_proc_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        -- 2.3. Insert into Temporary Table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_HKF_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''', v_company_code, ''', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''', v_company_code, ''' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''');'
        );
        
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- 2.4. Apply Modification Rules
        
        IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            
            -- [LTR Logic]
            IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
                /*
                    Rule 1: 맨 마지막열 값 추가(2개)
                    ① 항목명I : 납기구분 / 항목값 : 년납
                    ② 항목명II : 납입월 / 항목값 : 해당월(ex.202512)    
                */
                -- Rule 1.1: 항목명I : 납기구분 / 항목값 : 년납
                UPDATE T_TEMP_RPA_HKF_PROCESSED 
                SET COLUMN_31 = '년납';

                -- Rule 1.2: 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
                UPDATE T_TEMP_RPA_HKF_PROCESSED 
                SET COLUMN_32 = v_target_ym;

                /*
                    Rule 2: 중복 증번 편집
                    ① 맨아래 계 부분의 데이터 행2개 삭제
                    ② [계약번호] 오름차순 정렬
                    ③ 중복 계약번호 중 [상태]=각각"정상,철회/인수거부"이면 [상태]="정상" 데이터 행삭제
                    ④ [영수보험료],[수정보험료]="마이너스 금액"이면 "플러스 금액"으로 값수정
                */
                -- Rule 2.1: 맨아래 계 부분의 데이터 행2개 삭제
                DELETE FROM T_TEMP_RPA_HKF_PROCESSED
                WHERE COLUMN_01 IS NULL 
                OR COLUMN_01 = ''
                OR COLUMN_01 NOT REGEXP '^[0-9]+$';

                -- Rule 2.2: [계약번호] 오름차순 정렬
                SET @seq := 0;
                UPDATE T_TEMP_RPA_HKF_PROCESSED
                SET SORT_ORDER_NO = (@seq := @seq + 1)
                ORDER BY COLUMN_04 ASC;

                -- Rule 2.3: 중복 계약번호 중 [상태]=각각"정상,철회/인수거부"이면 [상태]="정상" 데이터 행삭제
                DROP TEMPORARY TABLE IF EXISTS tmp_dup_chulhoe_hkf;
                CREATE TEMPORARY TABLE tmp_dup_chulhoe_hkf (seq_no VARCHAR(150));
                INSERT INTO tmp_dup_chulhoe_hkf (seq_no)
                SELECT COLUMN_04 FROM T_TEMP_RPA_HKF_PROCESSED GROUP BY COLUMN_04
                HAVING SUM(COLUMN_11 IN ('철회/인수거부', '철회', '인수거부')) > 0 
                AND SUM(COLUMN_11 = '정상') > 0;

                DELETE t FROM T_TEMP_RPA_HKF_PROCESSED t 
                INNER JOIN tmp_dup_chulhoe_hkf d ON t.COLUMN_04 = d.seq_no 
                WHERE t.COLUMN_11 = '정상';

                DROP TEMPORARY TABLE IF EXISTS tmp_dup_chulhoe_hkf;

                -- Rule 2.4: [영수보험료],[수정보험료]="마이너스 금액"이면 "플러스 금액"으로 값수정
                UPDATE T_TEMP_RPA_HKF_PROCESSED 
                SET COLUMN_14 = CAST(ABS(CAST(REPLACE(IFNULL(COLUMN_14,'0'), ',', '') AS SIGNED)) AS CHAR) 
                WHERE REPLACE(COLUMN_14, ',', '') REGEXP '^-[0-9]+';
                
                UPDATE T_TEMP_RPA_HKF_PROCESSED 
                SET COLUMN_22 = CAST(ABS(CAST(REPLACE(IFNULL(COLUMN_22,'0'), ',', '') AS SIGNED)) AS CHAR) 
                WHERE REPLACE(COLUMN_22, ',', '') REGEXP '^-[0-9]+';

                /* 
                    Rule 3: [계약일자]≠"해당월"면 데이터 행삭제 
                */
                DELETE FROM T_TEMP_RPA_HKF_PROCESSED 
                WHERE LEFT(REPLACE(REPLACE(COLUMN_03, '-', ''), '.', ''), 6) <> v_target_ym;

                /* 
                    Rule 4: [납기]="세납"인 경우 → T_RPA_INSURANCE_EXTRA_GUIDE 참조하여 업데이트 
                */
                UPDATE T_TEMP_RPA_HKF_PROCESSED a
                INNER JOIN T_RPA_INSURANCE_EXTRA_GUIDE b
                ON
                    a.COLUMN_04 = b.SEARCH_DATA
                    AND b.SYS_FLAG = '1'
                    AND b.BATCH_ID = IN_BATCH_ID
                    AND b.COMPANY_CODE = v_company_code
                    AND b.INSURANCE_TYPE = IN_INSURANCE_TYPE
                    AND b.CONTRACT_TYPE = IN_CONTRACT_TYPE
                    AND b.BUSINESS_RULE_NO = 4
                    AND b.COLUMN_NAME = '납기'
                    AND b.ACTION = 'UPD'
                SET a.COLUMN_23 = b.AFTER_COLUMN_DATA;
                
            -- [CAR Logic]
            ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN

                /* 
                    Rule 1: 맨 마지막열 값 추가(5개)
                    ① 항목명I : 납기구분 / 항목값 : 년납
                    ② 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
                    ③ 항목명III : 납입일 / 항목값 : 영수일과 동일한 값으로 반영
                    ④ 항목명IV : 만기일자 / 항목값 : 증번별로 원부확인하여 데이터반영
                    ⑤ 항목명V : 차량번호 / 증번별로 원부확인하여 데이터반영
                    ※ 전체 행에 반영
                */
                -- Rule 1.1: 항목명I : 납기구분 / 항목값 : 년납
                UPDATE T_TEMP_RPA_HKF_PROCESSED
                SET COLUMN_31 = '년납';
                -- Rule 1.2: 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
                UPDATE T_TEMP_RPA_HKF_PROCESSED
                SET COLUMN_32 = v_target_ym;
                -- Rule 1.3: 항목명III : 납입일 / 항목값 : 영수일과 동일한 값으로 반영
                UPDATE T_TEMP_RPA_HKF_PROCESSED
                SET COLUMN_33 = COLUMN_02;

                -- Rule 1.4: 항목명IV : 만기일자 / 항목값 : 증번별로 원부확인하여 데이터반영
                UPDATE T_TEMP_RPA_HKF_PROCESSED a
                INNER JOIN T_RPA_INSURANCE_EXTRA_GUIDE b
                ON
                    a.COLUMN_04 = b.SEARCH_DATA
                    AND b.SYS_FLAG = '1'
                    AND b.BATCH_ID = IN_BATCH_ID
                    AND b.COMPANY_CODE = v_company_code
                    AND b.INSURANCE_TYPE = IN_INSURANCE_TYPE
                    AND b.CONTRACT_TYPE = IN_CONTRACT_TYPE
                    AND b.BUSINESS_RULE_NO = 1
                    AND b.COLUMN_NAME = '만기일자'
                    AND b.ACTION = 'ADD'
                SET a.COLUMN_34 = b.AFTER_COLUMN_DATA;

                -- Rule 1.5: 항목명V : 차량번호 / 증번별로 원부확인하여 데이터반영
                UPDATE T_TEMP_RPA_HKF_PROCESSED a
                INNER JOIN T_RPA_INSURANCE_EXTRA_GUIDE b
                ON
                    a.COLUMN_04 = b.SEARCH_DATA
                    AND b.SYS_FLAG = '1'
                    AND b.BATCH_ID = IN_BATCH_ID
                    AND b.COMPANY_CODE = v_company_code
                    AND b.INSURANCE_TYPE = IN_INSURANCE_TYPE
                    AND b.CONTRACT_TYPE = IN_CONTRACT_TYPE
                    AND b.BUSINESS_RULE_NO = 1
                    AND b.COLUMN_NAME = '차량번호'
                    AND b.ACTION = 'ADD'
                SET a.COLUMN_35 = b.AFTER_COLUMN_DATA;

                /* 
                    Rule 2: 중복 증번 편집
                    ① 맨아래 계 부분의 데이터 행2개 삭제
                    ② [증권번호] 오름차순 정렬
                    ③ 중복 증권번호 중 [합계보험료]="0"은 데이터 행삭제하고 [합계보험료]≠"0" 데이터는 [상태]="철회"로 값수정
                */
                -- Rule 2.1: 맨아래 계 부분의 데이터 행2개 삭제
                DELETE FROM T_TEMP_RPA_HKF_PROCESSED
                WHERE COLUMN_01 IS NULL 
                OR COLUMN_01 = ''
                OR COLUMN_01 NOT REGEXP '^[0-9]+$';

                -- Rule 2.2: [증권번호] 오름차순 정렬
                SET @seq := 0;
                UPDATE T_TEMP_RPA_HKF_PROCESSED
                SET SORT_ORDER_NO = (@seq := @seq + 1)
                ORDER BY COLUMN_04 ASC;

                -- Rule 2.3: 중복 증권번호 중 [합계보험료]="0"은 데이터 행삭제하고 [합계보험료]≠"0" 데이터는 [상태]="철회"로 값수정
                DROP TEMPORARY TABLE IF EXISTS tmp_dup_gen_hkf;
                CREATE TEMPORARY TABLE tmp_dup_gen_hkf (seq_no VARCHAR(150));
                INSERT INTO tmp_dup_gen_hkf (seq_no)
                SELECT COLUMN_04 FROM T_TEMP_RPA_HKF_PROCESSED
                GROUP BY COLUMN_04 HAVING COUNT(*) > 1;

                DELETE t FROM T_TEMP_RPA_HKF_PROCESSED t
                INNER JOIN tmp_dup_gen_hkf d ON t.COLUMN_04 = d.seq_no
                WHERE CAST(REPLACE(IFNULL(t.COLUMN_15,'0'), ',', '') AS DECIMAL(20,2)) = 0;

                UPDATE T_TEMP_RPA_HKF_PROCESSED t
                INNER JOIN tmp_dup_gen_hkf d ON t.COLUMN_04 = d.seq_no
                SET t.COLUMN_11 = '철회'
                WHERE CAST(REPLACE(IFNULL(t.COLUMN_15,'0'), ',', '') AS DECIMAL(20,2)) <> 0;

                DROP TEMPORARY TABLE IF EXISTS tmp_dup_gen_hkf;

            -- [GEN Logic]
            ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
                /* 
                    Rule 1: 맨 마지막열 값 추가(6개)
                    ① 항목명I : 납기구분 / 항목값 : 년납
                    ② 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
                    ③ 항목명III : 납입주기 / 항목값 : 일시납
                    ④ 항목명IV : 만기일자 / 항목값 : 증번별로 원부확인하여 데이터반영
                    ⑤ 항목명V : 납기 / 항목값 : 0
                    ⑥ 항목명VI : 보험사성적 / 항목값 : 0
                    ※ 전체 행에 반영
                 */ 
                -- Rule 1.1: 항목명I : 납기구분 / 항목값 : 년납
                UPDATE T_TEMP_RPA_HKF_PROCESSED
                SET 
                    COLUMN_21 = '년납';

                -- Rule 1.2: 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
                UPDATE T_TEMP_RPA_HKF_PROCESSED
                SET
                    COLUMN_22 = v_target_ym;

                -- Rule 1.3: 항목명III : 납입주기 / 항목값 : 일시납
                UPDATE T_TEMP_RPA_HKF_PROCESSED
                SET
                    COLUMN_23 = '일시납';

                -- Rule 1.4: 항목명IV : 만기일자 / 항목값 : 증번별로 원부확인하여 데이터반영
                UPDATE T_TEMP_RPA_HKF_PROCESSED a
                INNER JOIN T_RPA_INSURANCE_EXTRA_GUIDE b
                ON
                    a.COLUMN_05 = b.SEARCH_DATA
                    AND b.SYS_FLAG = '1'
                    AND b.BATCH_ID = IN_BATCH_ID
                    AND b.COMPANY_CODE = v_company_code
                    AND b.INSURANCE_TYPE = IN_INSURANCE_TYPE
                    AND b.CONTRACT_TYPE = IN_CONTRACT_TYPE
                    AND b.BUSINESS_RULE_NO = 1
                    AND b.COLUMN_NAME = '만기일자'
                    AND b.ACTION = 'ADD'
                SET a.COLUMN_24 = b.AFTER_COLUMN_DATA;

                -- Rule 1.5: 항목명V : 납기 / 항목값 : 0
                UPDATE T_TEMP_RPA_HKF_PROCESSED
                SET
                    COLUMN_25 = '0';

                -- Rule 1.6: 항목명VI : 보험사성적 / 항목값 : 0
                UPDATE T_TEMP_RPA_HKF_PROCESSED
                SET
                    COLUMN_26 = '0';

                /*
                    Rule 2: 중복 증번 편집
                    ① 맨아래 계 부분의 데이터 행2개 삭제
                    ② [증권번호] 오름차순 정렬
                    ③ 중복 증권번호 중 [합계보험료]="0"은 데이터 행삭제하고 [합계보험료]≠"0" 데이터는 [상태]="철회"로 값수정
                    ④ [만기일자]은 원수사 원부에서 조회하여 값수정
                    → 원부확인 : 업무메뉴>계약>조회>"계약상세"→ "계약번호" 입력 후 조회 → " 만기일자" 확인
                */

                -- Rule 2.1: 맨아래 계 부분의 데이터 행2개 삭제
                DELETE FROM T_TEMP_RPA_HKF_PROCESSED
                WHERE COLUMN_01 IS NULL 
                OR COLUMN_01 = ''
                OR COLUMN_01 NOT REGEXP '^[0-9]+$';
                
                -- Rule 2.2: [계약번호] 오름차순 정렬
                SET @seq := 0;
                UPDATE T_TEMP_RPA_HKF_PROCESSED
                SET SORT_ORDER_NO = (@seq := @seq + 1)
                ORDER BY COLUMN_05 ASC;

                -- Rule 2.3: 중복 증권번호 중 [합계보험료]="0"은 데이터 행삭제하고 [합계보험료]≠"0" 데이터는 [상태]="철회"로 값수정
                DROP TEMPORARY TABLE IF EXISTS tmp_dup_gen_hkf;
                CREATE TEMPORARY TABLE tmp_dup_gen_hkf (seq_no VARCHAR(150));
                INSERT INTO tmp_dup_gen_hkf (seq_no) 
                SELECT COLUMN_05 FROM T_TEMP_RPA_HKF_PROCESSED 
                GROUP BY COLUMN_05 HAVING COUNT(*) > 1;

                DELETE t FROM T_TEMP_RPA_HKF_PROCESSED t 
                INNER JOIN tmp_dup_gen_hkf d ON t.COLUMN_05 = d.seq_no 
                WHERE CAST(REPLACE(IFNULL(t.COLUMN_15,'0'), ',', '') AS DECIMAL(20,2)) = 0;

                UPDATE T_TEMP_RPA_HKF_PROCESSED t 
                INNER JOIN tmp_dup_gen_hkf d ON t.COLUMN_05 = d.seq_no 
                SET t.COLUMN_11 = '철회' 
                WHERE CAST(REPLACE(IFNULL(t.COLUMN_15,'0'), ',', '') AS DECIMAL(20,2)) <> 0;

                DROP TEMPORARY TABLE IF EXISTS tmp_dup_gen_hkf;
            END IF;
        END IF;

        -- 2.5. Build sql query insert processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_proc_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_HKF_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        -- 2.6. Drop temporary table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_HKF_PROCESSED;

    END IF;

END