CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_SSF`(
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
    DECLARE v_company_code VARCHAR(10)  DEFAULT 'SSF';
    DECLARE v_target_ym    VARCHAR(6)   DEFAULT '';

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_SSF_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_tae_a_agg;
        DROP TEMPORARY TABLE IF EXISTS tmp_sorted_ssf;
    END;

    -- [SET logic]
    SET v_target_ym = DATE_FORMAT(NOW(), '%Y%m');

    -- 1. Hardcoded Column Mapping for Samsung Fire (SSF)
    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        -- Mapping for NEW contracts (Columns 01-66 + Target-only 67, 68)
        SET v_raw_cols = ''; SET v_proc_cols = '';
        
        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_01, ', -- 계약번호
            'COLUMN_02, ', -- 상품명
            'COLUMN_03, '); -- 상품코드
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_01, ', -- 계약번호
            'COLUMN_02, ', -- 상품명
            'COLUMN_03, '); -- 상품코드
        
        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_04, ', -- 계약자명
            'COLUMN_05, ', -- 피보험자명
            'COLUMN_06, '); -- 납입회차
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_04, ', -- 계약자명
            'COLUMN_05, ', -- 피보험자명
            'COLUMN_06, '); -- 납입회차
        
        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_07, ', -- 대체계약여부
            'COLUMN_08, ', -- 보험료
            'COLUMN_09, '); -- 월납보험료
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_07, ', -- 대체계약여부
            'COLUMN_08, ', -- 보험료
            'COLUMN_09, '); -- 월납보험료
        
        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_10, ', -- 평가실적
            'COLUMN_11, ', -- 월납환산수정P
            'COLUMN_12, '); -- 책임보험료
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_10, ', -- 평가실적
            'COLUMN_11, ', -- 월납환산수정P
            'COLUMN_12, '); -- 책임보험료
        
        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_13, ', -- 임의보험료
            'COLUMN_14, ', -- 커미션계
            'COLUMN_15, '); -- 신계약/계약관리
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_13, ', -- 임의보험료
            'COLUMN_14, ', -- 커미션계
            'COLUMN_15, '); -- 신계약/계약관리
        
        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_16, ', -- 선지급
            'COLUMN_17, ', -- 유지
            'COLUMN_18, '); -- 책임
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_16, ', -- 선지급
            'COLUMN_17, ', -- 유지
            'COLUMN_18, '); -- 책임
        
        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_19, ', -- 임의
            'COLUMN_20, ', -- 순번
            'COLUMN_21, '); -- 담보패키지
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_19, ', -- 임의
            'COLUMN_20, ', -- 순번
            'COLUMN_21, '); -- 담보패키지
        
        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_22, ', -- 계약상태
            'COLUMN_23, ', -- 정상집금여부
            'COLUMN_24, '); -- 장기청약일
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_22, ', -- 계약상태
            'COLUMN_23, ', -- 정상집금여부
            'COLUMN_24, '); -- 장기청약일
        
        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_25, ', -- 보험시기
            'COLUMN_26, ', -- 보험종기
            'COLUMN_27, '); -- 마감일
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_25, ', -- 보험시기
            'COLUMN_26, ', -- 보험종기
            'COLUMN_27, '); -- 마감일
        
        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_28, ', -- 영수일
            'COLUMN_29, ', -- 응당일
            'COLUMN_30, '); -- 보험기간
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_28, ', -- 영수일
            'COLUMN_29, ', -- 응당일
            'COLUMN_30, '); -- 보험기간
        
        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_31, ', -- 납입기간
            'COLUMN_32, ', -- 납입주기
            'COLUMN_33, '); -- 집금방법
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_31, ', -- 납입기간
            'COLUMN_32, ', -- 납입주기
            'COLUMN_33, '); -- 집금방법
        
        -- 34-36
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_34, ', -- 장기갱신주기
            'COLUMN_35, ', -- 갱신회차
            'COLUMN_36, '); -- 수금성과구분
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_34, ', -- 장기갱신주기
            'COLUMN_35, ', -- 갱신회차
            'COLUMN_36, '); -- 수금성과구분
        
        -- 37-39
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_37, ', -- 본인/이관
            'COLUMN_38, ', -- 18년이후이관여부
            'COLUMN_39, '); -- RC계약여부
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_37, ', -- 본인/이관
            'COLUMN_38, ', -- 18년이후이관여부
            'COLUMN_39, '); -- RC계약여부
        
        -- 40-42
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_40, ', -- 장기상품구분
            'COLUMN_41, ', -- 보장新수정P
            'COLUMN_42, '); -- 보장P_수정P
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_40, ', -- 장기상품구분
            'COLUMN_41, ', -- 보장新수정P
            'COLUMN_42, '); -- 보장P_수정P
        
        -- 43-45
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_43, ', -- 자동차비사업/사업
            'COLUMN_44, ', -- 자동차갱신여부
            'COLUMN_45, '); -- AIP
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_43, ', -- 자동차비사업/사업
            'COLUMN_44, ', -- 자동차갱신여부
            'COLUMN_45, '); -- AIP
        
        -- 46-48
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_46, ', -- 차종
            'COLUMN_47, ', -- 성과인정여부
            'COLUMN_48, '); -- 차량번호
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_46, ', -- 차종
            'COLUMN_47, ', -- 성과인정여부
            'COLUMN_48, '); -- 차량번호
        
        -- 49-51
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_49, ', -- 일반고정/변동
            'COLUMN_50, ', -- 출재율
            'COLUMN_51, '); -- 출재수수료율
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_49, ', -- 일반고정/변동
            'COLUMN_50, ', -- 출재율
            'COLUMN_51, '); -- 출재수수료율
        
        -- 52-54
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_52, ', -- 예정사업비율
            'COLUMN_53, ', -- 재원한도(G/L)
            'COLUMN_54, '); -- 일반할인율
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_52, ', -- 예정사업비율
            'COLUMN_53, ', -- 재원한도(G/L)
            'COLUMN_54, '); -- 일반할인율
        
        -- 55-57
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_55, ', -- 글로벌여부
            'COLUMN_56, ', -- 브로커여부
            'COLUMN_57, '); -- 공동인수
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_55, ', -- 글로벌여부
            'COLUMN_56, ', -- 브로커여부
            'COLUMN_57, '); -- 공동인수
        
        -- 58-60
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_58, ', -- 일반마스터증번
            'COLUMN_59, ', -- 지사코드
            'COLUMN_60, '); -- 지사명
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_58, ', -- 일반마스터증번
            'COLUMN_59, ', -- 지사코드
            'COLUMN_60, '); -- 지사명
        
        -- 61-63
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_61, ', -- 사용인코드
            'COLUMN_62, ', -- 사용인명
            'COLUMN_63, '); -- 모집자코드
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_61, ', -- 사용인코드
            'COLUMN_62, ', -- 사용인명
            'COLUMN_63, '); -- 모집자코드
        
        -- 64-66
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_64, ', -- 모집사용인
            'COLUMN_65, ', -- 전환여부
            'COLUMN_66, '); -- Case_ID
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_64, ', -- 모집사용인
            'COLUMN_65, ', -- 전환여부
            'COLUMN_66, '); -- Case_ID

        -- 67-68 (Target specific)
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
            SET v_raw_cols = CONCAT(v_raw_cols, 'NULL, NULL');
            SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_67, COLUMN_68');
        ELSE
            SET v_raw_cols = CONCAT(v_raw_cols, 'NULL');
            SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_67');
        END IF;

    END IF;

    -- 2. Execute dynamic mapping IF mapping exists
    IF v_raw_cols != '' AND v_proc_cols != '' THEN
        
        -- Select Tables based on Insurance Type
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

        -- Create Temporary Table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_SSF_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_SSF_PROCESSED LIKE ', v_proc_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        -- Insert into Temporary Table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_SSF_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''SSF'', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''SSF'' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''');'
        );
        
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- 3. Apply Transformation Logic
        
        -- [LTR Logic]
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            -- Rule 1: 맨 마지막열 값 추가(2개)
            -- ① 항목명I : 납기구분 / 항목값 : 년납
            -- ② 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
            -- ※ 전체 행에 반영
            UPDATE T_TEMP_RPA_SSF_PROCESSED SET COLUMN_67 = '년납', COLUMN_68 = v_target_ym;

            -- Rule 2: [계약상태]편집
            -- ① [계약상태]≠"신계약,취소,해지"이면 데이터 행삭제
            DELETE FROM T_TEMP_RPA_SSF_PROCESSED WHERE COLUMN_22 NOT IN ('신계약', '취소', '해지');

            -- ② [계약상태]="해지,취소" & [장기청약일]≠해당월이면 데이터 행삭제
            DELETE FROM T_TEMP_RPA_SSF_PROCESSED 
            WHERE COLUMN_22 IN ('해지', '취소') AND LEFT(REPLACE(COLUMN_24, '-', ''), 6) <> v_target_ym;

            -- Rule 3: 계약번호 중복 편집
            -- ① 계약번호 오름차순 정렬
            DROP TEMPORARY TABLE IF EXISTS tmp_sorted_ssf;
            CREATE TEMPORARY TABLE tmp_sorted_ssf LIKE T_TEMP_RPA_SSF_PROCESSED;
            INSERT INTO tmp_sorted_ssf SELECT * FROM T_TEMP_RPA_SSF_PROCESSED ORDER BY COLUMN_01 ASC;
            DELETE FROM T_TEMP_RPA_SSF_PROCESSED;
            INSERT INTO T_TEMP_RPA_SSF_PROCESSED SELECT * FROM tmp_sorted_ssf;

            -- ②:중복 계약번호 중 [피보험자]="태아"가 있는 경우 "보험료, 월납환산수정P"항목은 합산하여 한건으로 값수정
            SET @seq := 0;
            UPDATE T_TEMP_RPA_SSF_PROCESSED SET SORT_ORDER_NO = (@seq := @seq + 1) ORDER BY COLUMN_01 ASC;

            DROP TEMPORARY TABLE IF EXISTS tmp_tae_a_agg;
            CREATE TEMPORARY TABLE tmp_tae_a_agg
            SELECT 
                COLUMN_01 as policy_no,
                CAST(SUM(CAST(REPLACE(REPLACE(IFNULL(COLUMN_08, '0'), ',', ''), '.', '') AS SIGNED)) AS CHAR) as sum_c08,
                CAST(SUM(CAST(REPLACE(REPLACE(IFNULL(COLUMN_11, '0'), ',', ''), '.', '') AS SIGNED)) AS CHAR) as sum_c11,
                CASE WHEN SUM(COLUMN_05 = '태아') > 0 THEN '태아' ELSE MAX(CASE WHEN COLUMN_05 IS NULL OR COLUMN_05 = '' THEN COLUMN_04 ELSE COLUMN_05 END) END as final_name,
                MIN(SYS_ID) as first_id
            FROM T_TEMP_RPA_SSF_PROCESSED GROUP BY COLUMN_01 HAVING COUNT(*) > 1 AND SUM(COLUMN_05 = '태아') > 0;

            UPDATE T_TEMP_RPA_SSF_PROCESSED t INNER JOIN tmp_tae_a_agg agg ON t.SYS_ID = agg.first_id 
            SET t.COLUMN_08 = agg.sum_c08, t.COLUMN_11 = agg.sum_c11, t.COLUMN_05 = agg.final_name;
            DELETE t FROM T_TEMP_RPA_SSF_PROCESSED t INNER JOIN tmp_tae_a_agg agg ON t.COLUMN_01 = agg.policy_no WHERE t.SYS_ID <> agg.first_id;

            -- Rule 4: 상품명 원수사 원부확인하여 값수정    (장기>계약상세조회>"특성조회항목" → 상품명 확인) (Pause/Skip)
            
            -- Rule 5: [납입기간]="0"이면 원수사 원부확인하여 값수정
            -- → 장기>계약상세조회>"납입정보의 전체 회 / 12"계산한 값 (Pause/Skip)

        -- [CAR Logic]
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            -- Rule 1: 맨 마지막열 값 추가(1개)
            -- ① 항목명I : 납기구분 / 항목값 : 년납
            UPDATE T_TEMP_RPA_SSF_PROCESSED SET COLUMN_67 = '년납';
            
            -- Rule 2: [계약상태]="배서"면, 데이터 행삭제
            DELETE FROM T_TEMP_RPA_SSF_PROCESSED WHERE COLUMN_22 = '배서';

            -- Rule 3: [계약상태]="공란"이면 [계약상태]="신계약"으로 값수정
            UPDATE T_TEMP_RPA_SSF_PROCESSED SET COLUMN_22 = '신계약' WHERE COLUMN_22 IS NULL OR COLUMN_22 = '';
            
            -- Rule 4: 계약번호 중복 편집
            -- ① 계약번호 오름차순 정렬
            -- ② 중복 계약번호 중 [계약상태]=모두 "배서"면 해당 데이터들 행삭제
            -- ③ 중복 계약번호 중 [계약상태]=각각"신계약,배서"면 "배서" 데이터 행삭제
            DELETE FROM T_TEMP_RPA_SSF_PROCESSED WHERE COLUMN_01 IN (SELECT * FROM (SELECT COLUMN_01 FROM T_TEMP_RPA_SSF_PROCESSED GROUP BY COLUMN_01 HAVING COUNT(*)>1 AND SUM(COLUMN_22 <> '배서') = 0) as t);
            DELETE FROM T_TEMP_RPA_SSF_PROCESSED WHERE COLUMN_22 = '배서' AND COLUMN_01 IN (SELECT * FROM (SELECT COLUMN_01 FROM T_TEMP_RPA_SSF_PROCESSED WHERE COLUMN_22 = '신계약') as t);
            
            -- Rule 5: [보험료]="마이너스"이면 "플러스"값으로 수정
            UPDATE T_TEMP_RPA_SSF_PROCESSED SET COLUMN_08 = CAST(ABS(CAST(REPLACE(REPLACE(IFNULL(COLUMN_08, '0'), ',', ''), '.', '') AS SIGNED)) AS CHAR) WHERE COLUMN_08 LIKE '-%';

        -- [GEN Logic]
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            -- Rule 1: 맨 마지막열 값 추가(1개)
            -- ① 항목명I : 납기구분 / 항목값 : 년납
            UPDATE T_TEMP_RPA_SSF_PROCESSED SET COLUMN_67 = '년납';

            -- Rule 2: [계약상태]="배서"면 데이터 행삭제
            DELETE FROM T_TEMP_RPA_SSF_PROCESSED WHERE COLUMN_22 = '배서';

            -- Rule 3: [계약상태]="공란"이면 "신계약"으로 수정
            UPDATE T_TEMP_RPA_SSF_PROCESSED SET COLUMN_22 = '신계약' WHERE COLUMN_22 IS NULL OR COLUMN_22 = '';

            -- Rule 4: 계약번호 중복 편집
            -- ① 계약번호 오름차순 정렬
            -- ② 중복 계약번호 중 [계약상태]=모두 "배서"면 해당 데이터들 행삭제
            -- ③ 중복 계약번호 중 [계약상태]=각각"신계약,배서"면 "배서" 데이터 행삭제
            DELETE FROM T_TEMP_RPA_SSF_PROCESSED WHERE COLUMN_01 IN (SELECT * FROM (SELECT COLUMN_01 FROM T_TEMP_RPA_SSF_PROCESSED GROUP BY COLUMN_01 HAVING COUNT(*)>1 AND SUM(COLUMN_22 <> '배서') = 0) as t);
            DELETE FROM T_TEMP_RPA_SSF_PROCESSED WHERE COLUMN_22 = '배서' AND COLUMN_01 IN (SELECT * FROM (SELECT COLUMN_01 FROM T_TEMP_RPA_SSF_PROCESSED WHERE COLUMN_22 = '신계약') as t);

            -- Rule 5: [보험료]="마이너스"이면 "플러스"값으로 수정
            UPDATE T_TEMP_RPA_SSF_PROCESSED SET COLUMN_08 = CAST(ABS(CAST(REPLACE(REPLACE(IFNULL(COLUMN_08, '0'), ',', ''), '.', '') AS SIGNED)) AS CHAR) WHERE COLUMN_08 LIKE '-%';
        END IF;

        -- 4. Final Insert into Processed Table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_proc_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_SSF_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();

        -- 5. Cleanup
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_SSF_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_tae_a_agg;
        DROP TEMPORARY TABLE IF EXISTS tmp_sorted_ssf;

    END IF;

END